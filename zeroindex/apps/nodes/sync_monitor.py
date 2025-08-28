import asyncio
import logging
import requests
from typing import Dict, Any, Optional
from datetime import datetime
from django.utils import timezone
from django.db import transaction
from .models import Node

logger = logging.getLogger(__name__)


class NodeSyncMonitor:
    """Monitor sync progress of Ethereum nodes"""
    
    def __init__(self):
        self.timeout = 10  # Request timeout in seconds
    
    async def monitor_all_nodes(self):
        """Monitor sync status of all active nodes"""
        nodes = Node.objects.filter(status__in=['syncing', 'running'])
        
        logger.info(f"Monitoring {nodes.count()} active nodes")
        
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
            if node.is_ethereum_l1 and node.consensus_api_url:
                cons_status = await self.get_consensus_sync_status(node)
            
            # Update node status in database
            await self.update_node_status(node, exec_status, cons_status)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to monitor node {node.name}: {e}")
            return False
    
    async def get_execution_sync_status(self, node: Node) -> Dict[str, Any]:
        """Get sync status from execution client RPC"""
        if not node.execution_rpc_url:
            return {'error': 'No RPC URL configured'}
        
        try:
            # Try eth_syncing first
            sync_data = await self.rpc_call(node.execution_rpc_url, 'eth_syncing', [])
            
            if sync_data is False:
                # Node is fully synced
                block_number = await self.rpc_call(node.execution_rpc_url, 'eth_blockNumber', [])
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
        if not node.consensus_api_url:
            return {'error': 'No consensus API URL configured'}
        
        try:
            # Get sync status
            sync_url = f"{node.consensus_api_url}/eth/v1/node/syncing"
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
        
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _update)


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
    try:
        node = Node.objects.get(name=node_name)
    except Node.DoesNotExist:
        return {'error': f'Node "{node_name}" not found'}
    
    monitor = NodeSyncMonitor()
    
    # Run the async monitoring in a new event loop
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        success = loop.run_until_complete(monitor.monitor_node(node))
        
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
            
    finally:
        loop.close()


def monitor_all_nodes() -> Dict[str, Any]:
    """Monitor all active nodes (synchronous)"""
    monitor = NodeSyncMonitor()
    
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        loop.run_until_complete(monitor.monitor_all_nodes())
        
        # Get summary
        nodes = Node.objects.filter(status__in=['syncing', 'running'])
        return {
            'success': True,
            'monitored_nodes': nodes.count(),
            'summary': {
                'syncing': nodes.filter(status='syncing').count(),
                'running': nodes.filter(status='running').count(),
            }
        }
        
    finally:
        loop.close()