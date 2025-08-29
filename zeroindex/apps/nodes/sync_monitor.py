import asyncio
import logging
import requests
from typing import Dict, Any, Optional
from datetime import datetime
from django.utils import timezone
from django.db import transaction
from asgiref.sync import sync_to_async
from .models import Node

logger = logging.getLogger(__name__)


class NodeSyncMonitor:
    """Monitor sync progress of Ethereum nodes"""
    
    def __init__(self):
        self.timeout = 10  # Request timeout in seconds
    
    async def monitor_all_nodes(self):
        """Monitor sync status of all active nodes"""
        nodes = await sync_to_async(list)(Node.objects.filter(status__in=['syncing', 'running']))
        
        logger.info(f"Monitoring {len(nodes)} active nodes")
        
        tasks = []
        for node in nodes:
            task = asyncio.create_task(self.monitor_node(node))
            tasks.append(task)
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    
    async def monitor_node(self, node: Node) -> bool:
        """Monitor sync status of a single node"""
        try:
            logger.debug(f"Monitoring node {node.name}")
            
            # Get execution client sync status
            exec_status = await self.get_execution_sync_status(node)
            
            # Get consensus client sync status if it's an Ethereum L1 node
            cons_status = None
            is_eth_l1 = await sync_to_async(lambda: node.is_ethereum_l1)()
            consensus_url = await sync_to_async(lambda: node.consensus_api_url)()
            
            if is_eth_l1 and consensus_url:
                cons_status = await self.get_consensus_sync_status(node)
            
            # Update node status in database
            await self.update_node_status(node, exec_status, cons_status)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to monitor node {node.name}: {e}")
            return False
    
    async def get_execution_sync_status(self, node: Node) -> Dict[str, Any]:
        """Get sync status from execution client metrics or RPC"""
        rpc_url = await sync_to_async(lambda: node.execution_rpc_url)()
        if not rpc_url:
            return {'error': 'No RPC URL configured'}
        
        try:
            # First try to get sync status from metrics endpoint (works better with snap sync)
            if ':6060' in rpc_url:
                # Direct metrics endpoint
                metrics_url = rpc_url + '/debug/metrics'
            else:
                # Convert RPC URL to metrics URL
                metrics_url = rpc_url.replace(':8545', ':6060') + '/debug/metrics'
            
            try:
                metrics_data = await self.http_get(metrics_url)
                if metrics_data:
                    return await self.parse_geth_metrics(metrics_data)
            except Exception as metrics_error:
                logger.debug(f"Metrics endpoint failed for {node.name}, trying RPC: {metrics_error}")
                # If we're using the metrics port, don't try RPC fallback
                if ':6060' in rpc_url:
                    return {'error': f'Metrics endpoint failed: {metrics_error}'}
            
            # Fallback to RPC eth_syncing
            sync_data = await self.rpc_call(rpc_url, 'eth_syncing', [])
            
            if sync_data is False:
                # Node is fully synced
                block_number = await self.rpc_call(rpc_url, 'eth_blockNumber', [])
                return {
                    'is_syncing': False,
                    'sync_progress': 100.0,
                    'current_block': int(block_number, 16) if block_number else 0,
                    'highest_block': int(block_number, 16) if block_number else 0,
                }
            
            elif isinstance(sync_data, dict):
                # Node is syncing
                current_block = int(sync_data.get('currentBlock', '0x0'), 16)
                highest_block = int(sync_data.get('highestBlock', '0x0'), 16)
                
                if highest_block > 0:
                    sync_progress = (current_block / highest_block) * 100
                else:
                    sync_progress = 0.0
                
                return {
                    'is_syncing': True,
                    'sync_progress': min(sync_progress, 99.9),  # Cap at 99.9% while syncing
                    'current_block': current_block,
                    'highest_block': highest_block,
                    'starting_block': int(sync_data.get('startingBlock', '0x0'), 16),
                }
            
            else:
                return {'error': f'Unexpected sync response: {sync_data}'}
                
        except Exception as e:
            logger.error(f"Error getting execution sync status for {node.name}: {e}")
            return {'error': str(e)}
    
    async def get_consensus_sync_status(self, node: Node) -> Dict[str, Any]:
        """Get sync status from consensus client API"""
        consensus_url = await sync_to_async(lambda: node.consensus_api_url)()
        if not consensus_url:
            return {'error': 'No consensus API URL configured'}
        
        try:
            # Get sync status
            sync_url = f"{consensus_url}/eth/v1/node/syncing"
            response = await self.http_get(sync_url)
            
            if not response:
                return {'error': 'No response from consensus API'}
            
            data = response.get('data', {})
            head_slot = int(data.get('head_slot', 0))
            sync_distance = int(data.get('sync_distance', 0))
            is_syncing = data.get('is_syncing', True)
            
            # Calculate sync progress
            if not is_syncing:
                sync_progress = 100.0
            elif sync_distance > 0:
                sync_progress = max(0, min(99.9, ((head_slot / (head_slot + sync_distance)) * 100)))
            else:
                sync_progress = 0.0
            
            return {
                'is_syncing': is_syncing,
                'sync_progress': sync_progress,
                'head_slot': head_slot,
                'sync_distance': sync_distance,
            }
            
        except Exception as e:
            logger.error(f"Error getting consensus sync status for {node.name}: {e}")
            return {'error': str(e)}
    
    async def rpc_call(self, url: str, method: str, params: list) -> Any:
        """Make JSON-RPC call to execution client"""
        payload = {
            'jsonrpc': '2.0',
            'method': method,
            'params': params,
            'id': 1
        }
        
        response = await self.http_post(url, payload)
        
        if 'error' in response:
            raise Exception(f"RPC error: {response['error']}")
        
        return response.get('result')
    
    async def http_get(self, url: str) -> Dict[str, Any]:
        """Make HTTP GET request"""
        loop = asyncio.get_event_loop()
        
        def _request():
            response = requests.get(url, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        
        return await loop.run_in_executor(None, _request)
    
    async def parse_geth_metrics(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Parse Geth metrics to determine sync progress"""
        try:
            # Get current block from metrics
            current_block = metrics.get('chain/head/header', 0)
            
            # Estimate target block from download metrics or use a reasonable approximation
            # For mainnet, we can estimate based on average block time (~12s) and current timestamp
            import time
            genesis_timestamp = 1438269973  # Ethereum mainnet genesis
            current_timestamp = int(time.time())
            estimated_blocks_since_genesis = (current_timestamp - genesis_timestamp) // 12
            estimated_current_block = estimated_blocks_since_genesis
            
            # Check if we're actively downloading (indicates sync in progress)
            bodies_downloading = metrics.get('eth/downloader/bodies/in.one-minute', 0) > 0
            headers_downloading = metrics.get('eth/downloader/headers/in.one-minute', 0) > 0
            receipts_downloading = metrics.get('eth/downloader/receipts/in.one-minute', 0) > 0
            
            # Check sync state metrics
            is_actively_syncing = any([
                bodies_downloading,
                headers_downloading, 
                receipts_downloading,
                current_block < (estimated_current_block - 100)  # More than 100 blocks behind
            ])
            
            if is_actively_syncing:
                # Calculate sync progress
                if estimated_current_block > 0:
                    sync_progress = (current_block / estimated_current_block) * 100
                    sync_progress = min(sync_progress, 99.9)  # Cap at 99.9% while syncing
                else:
                    sync_progress = 0.0
                
                return {
                    'is_syncing': True,
                    'sync_progress': sync_progress,
                    'current_block': current_block,
                    'highest_block': estimated_current_block,
                }
            else:
                # Node appears to be synced
                return {
                    'is_syncing': False,
                    'sync_progress': 100.0,
                    'current_block': current_block,
                    'highest_block': current_block,
                }
                
        except Exception as e:
            logger.error(f"Error parsing Geth metrics: {e}")
            return {'error': f'Failed to parse metrics: {e}'}
    
    async def http_post(self, url: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Make HTTP POST request"""
        loop = asyncio.get_event_loop()
        
        def _request():
            response = requests.post(
                url, 
                json=data, 
                timeout=self.timeout,
                headers={'Content-Type': 'application/json'}
            )
            response.raise_for_status()
            return response.json()
        
        return await loop.run_in_executor(None, _request)
    
    async def update_node_status(self, node: Node, exec_status: Dict[str, Any], cons_status: Optional[Dict[str, Any]] = None):
        """Update node sync status in database"""
        
        @sync_to_async
        def _update():
            with transaction.atomic():
                # Refresh node from database
                node.refresh_from_db()
                
                # Update execution client status
                if 'error' not in exec_status:
                    node.execution_sync_progress = exec_status.get('sync_progress', 0.0)
                    if 'current_block' in exec_status:
                        node.current_block_height = exec_status['current_block']
                
                # Update consensus client status
                if cons_status and 'error' not in cons_status:
                    node.consensus_sync_progress = cons_status.get('sync_progress', 0.0)
                    if 'head_slot' in cons_status:
                        node.consensus_head_slot = cons_status['head_slot']
                
                # Update overall node status
                if node.is_ethereum_l1:
                    # For Ethereum L1, both clients need to be considered
                    exec_synced = node.execution_sync_progress >= 99.9
                    cons_synced = node.consensus_sync_progress >= 99.9
                    
                    if exec_synced and cons_synced:
                        node.status = 'running'
                    elif node.status != 'provisioning':
                        node.status = 'syncing'
                else:
                    # For other chains, only execution client
                    if node.execution_sync_progress >= 99.9:
                        node.status = 'running'
                    elif node.status != 'provisioning':
                        node.status = 'syncing'
                
                node.last_health_check = timezone.now()
                node.save()
                
                logger.debug(f"Updated {node.name}: exec={node.execution_sync_progress:.1f}%, "
                           f"cons={node.consensus_sync_progress:.1f}% if {node.is_ethereum_l1} else 'N/A', "
                           f"status={node.status}")
        
        await _update()


class SyncMonitorService:
    """Service to run sync monitoring continuously"""
    
    def __init__(self, interval: int = 60):
        self.interval = interval  # Monitor interval in seconds
        self.monitor = NodeSyncMonitor()
        self.running = False
    
    async def start(self):
        """Start the monitoring service"""
        self.running = True
        logger.info(f"Starting sync monitor service (interval: {self.interval}s)")
        
        while self.running:
            try:
                await self.monitor.monitor_all_nodes()
                await asyncio.sleep(self.interval)
            except Exception as e:
                logger.error(f"Error in sync monitor loop: {e}")
                await asyncio.sleep(self.interval)
    
    def stop(self):
        """Stop the monitoring service"""
        self.running = False
        logger.info("Stopping sync monitor service")


# Convenience functions for management commands
def monitor_node_sync(node_name: str) -> Dict[str, Any]:
    """Monitor sync status of a specific node (synchronous)"""
    import threading
    import concurrent.futures
    
    try:
        node = Node.objects.get(name=node_name)
    except Node.DoesNotExist:
        return {'error': f'Node "{node_name}" not found'}
    
    monitor = NodeSyncMonitor()
    
    # Run in a separate thread to avoid async context conflicts
    def run_monitoring():
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            return loop.run_until_complete(monitor.monitor_node(node))
        finally:
            loop.close()
    
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future = executor.submit(run_monitoring)
        success = future.result()
        
        if success:
            node.refresh_from_db()
            return {
                'success': True,
                'node': node.name,
                'status': node.status,
                'execution_sync_progress': node.execution_sync_progress,
                'consensus_sync_progress': node.consensus_sync_progress,
                'current_block_height': node.current_block_height,
                'consensus_head_slot': node.consensus_head_slot,
                'last_health_check': node.last_health_check,
            }
        else:
            return {'error': 'Failed to monitor node'}


def monitor_all_nodes() -> Dict[str, Any]:
    """Monitor all active nodes (synchronous)"""
    import concurrent.futures
    
    monitor = NodeSyncMonitor()
    
    # Run in a separate thread to avoid async context conflicts
    def run_monitoring():
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(monitor.monitor_all_nodes())
            return True
        finally:
            loop.close()
    
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future = executor.submit(run_monitoring)
        success = future.result()
        
        if success:
            # Get summary
            nodes = Node.objects.filter(status__in=['syncing', 'running'])
            syncing_count = nodes.filter(status='syncing').count()
            running_count = nodes.filter(status='running').count()
            return {
                'success': True,
                'monitored_nodes': nodes.count(),
                'summary': {
                    'syncing': syncing_count,
                    'running': running_count,
                }
            }
        else:
            return {'error': 'Failed to monitor nodes'}