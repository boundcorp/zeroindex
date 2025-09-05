#!/usr/bin/env python3
"""
Advanced Ethereum Node Status Monitor
Handles all sync stages, edge cases, and provides detailed diagnostics
"""

import os
import sys
import time
import re
import django
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

# Setup Django
sys.path.append('/home/dev/p/boundcorp/zeroindex')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'zeroindex.settings.project')
django.setup()

from zeroindex.apps.nodes.models import Node
from zeroindex.apps.chains.models import Chain
import subprocess
import json

class EthereumNodeMonitor:
    def __init__(self):
        self.target_block = 23242692
        self.speed_history = []
        self.prev_block = None
        self.prev_time = None
        self.startup_time = datetime.now()
        self.geth_rpc = "http://10.43.71.202:8545"
        self.lighthouse_api = "http://10.43.14.75:5052"
        
    def get_pod_logs(self, pod_name: str, container: str, lines: int = 20) -> List[str]:
        """Get recent logs from a pod container"""
        try:
            cmd = f"kubectl logs -n devbox {pod_name} -c {container} --tail={lines}"
            result = subprocess.run(cmd.split(), capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                return result.stdout.strip().split('\n')
            return []
        except Exception:
            return []
    
    def check_geth_rpc_sync(self) -> Dict:
        """Check Geth sync status via RPC"""
        try:
            import requests
            response = requests.post(
                self.geth_rpc,
                json={"jsonrpc": "2.0", "method": "eth_syncing", "params": [], "id": 1},
                timeout=5
            )
            result = response.json().get('result')
            
            if result is False:
                # Not syncing means fully synced
                return {'is_syncing': False, 'fully_synced': True}
            elif result:
                # Extract sync details
                current = int(result.get('currentBlock', '0x0'), 16)
                highest = int(result.get('highestBlock', '0x0'), 16)
                tx_remaining = int(result.get('txIndexRemainingBlocks', '0x0'), 16)
                
                # Check if only transaction indexing remains
                is_fully_synced = (current >= highest - 10) and tx_remaining == 0
                
                return {
                    'is_syncing': True,
                    'fully_synced': is_fully_synced,
                    'current_block': current,
                    'highest_block': highest,
                    'tx_index_remaining': tx_remaining,
                    'behind_blocks': highest - current
                }
        except Exception:
            return {'is_syncing': None, 'fully_synced': False}
    
    def parse_geth_sync_status(self, logs: List[str]) -> Dict:
        """Parse Geth sync status from logs"""
        status = {
            'stage': 'unknown',
            'chain_progress': 0.0,
            'state_progress': 0.0,
            'snapshot_progress': 0.0,
            'tx_index_progress': 0.0,
            'log_index_progress': 0.0,
            'eta_minutes': None,
            'current_block': None,
            'highest_block': None,
            'state_accounts': 0,
            'state_nodes': 0,
            'pending_state': 0,
            'snapshot_accounts': 0,
            'snapshot_slots': 0,
            'tx_blocks_processed': 0,
            'tx_remaining': 0,
            'log_blocks_processed': 0,
            'log_remaining': 0,
            'last_activity': None,
            'substages': []  # Track multiple concurrent processes
        }
        
        for line in reversed(logs[-15:]):  # Check more lines for multiple processes
            # Chain download progress
            chain_match = re.search(r'chain download.*synced=(\d+\.\d+)%.*headers=([0-9,]+)@.*eta=(\d+)m(\d+)', line)
            if chain_match:
                if status['stage'] == 'unknown':
                    status['stage'] = 'chain_download'
                status['chain_progress'] = float(chain_match.group(1))
                status['current_block'] = int(chain_match.group(2).replace(',', ''))
                eta_min = int(chain_match.group(3))
                eta_sec = int(chain_match.group(4))
                status['eta_minutes'] = eta_min + eta_sec/60
                status['last_activity'] = self._extract_timestamp(line)
                continue
            
            # State healing progress
            state_match = re.search(r'state healing.*accounts=([0-9,]+)@.*nodes=([0-9,]+)@.*pending=([0-9,]+)', line)
            if state_match:
                if status['stage'] == 'unknown':
                    status['stage'] = 'state_healing'
                status['state_accounts'] = int(state_match.group(1).replace(',', ''))
                status['state_nodes'] = int(state_match.group(2).replace(',', ''))
                status['pending_state'] = int(state_match.group(3).replace(',', ''))
                status['last_activity'] = self._extract_timestamp(line)
                if 'state_healing' not in status['substages']:
                    status['substages'].append('state_healing')
                continue
            
            # Snapshot generation
            snapshot_match = re.search(r'Generating snapshot.*accounts=([0-9,]+).*slots=([0-9,]+).*eta=(\d+)h(\d+)m([0-9.]+)s', line)
            if snapshot_match:
                if status['stage'] == 'unknown' or status['stage'] == 'state_healing':
                    status['stage'] = 'post_healing'
                status['snapshot_accounts'] = int(snapshot_match.group(1).replace(',', ''))
                status['snapshot_slots'] = int(snapshot_match.group(2).replace(',', ''))
                eta_hours = int(snapshot_match.group(3))
                eta_mins = int(snapshot_match.group(4))
                status['eta_minutes'] = eta_hours * 60 + eta_mins
                status['last_activity'] = self._extract_timestamp(line)
                if 'snapshot_generation' not in status['substages']:
                    status['substages'].append('snapshot_generation')
                continue
            
            # Transaction indexing
            tx_match = re.search(r'Indexing transactions.*blocks=([0-9,]+).*txs=([0-9,]+).*total=([0-9,]+)', line)
            if tx_match:
                if status['stage'] == 'unknown' or status['stage'] == 'state_healing':
                    status['stage'] = 'post_healing'
                status['tx_blocks_processed'] = int(tx_match.group(1).replace(',', ''))
                total_tx = int(tx_match.group(3).replace(',', ''))
                current_tx = int(tx_match.group(2).replace(',', ''))
                status['tx_remaining'] = total_tx
                if total_tx > 0:
                    status['tx_index_progress'] = (current_tx / total_tx) * 100
                status['last_activity'] = self._extract_timestamp(line)
                if 'tx_indexing' not in status['substages']:
                    status['substages'].append('tx_indexing')
                continue
            
            # Log index rendering
            log_match = re.search(r'Log index.*processed=([0-9,]+).*remaining=([0-9,]+)', line)
            if log_match:
                if status['stage'] == 'unknown' or status['stage'] == 'state_healing':
                    status['stage'] = 'post_healing'
                processed = int(log_match.group(1).replace(',', ''))
                remaining = int(log_match.group(2).replace(',', ''))
                status['log_blocks_processed'] = processed
                status['log_remaining'] = remaining
                total = processed + remaining
                if total > 0:
                    status['log_index_progress'] = (processed / total) * 100
                status['last_activity'] = self._extract_timestamp(line)
                if 'log_indexing' not in status['substages']:
                    status['substages'].append('log_indexing')
                continue
            
            # Check for fully synced state (but not if indexing is still running)
            if 'Imported new chain segment' in line or 'Block synchronisation started' in line:
                # Only mark as fully synced if no background tasks are running
                if not status['substages'] or all(
                    status.get(f'{task}_progress', 0) >= 100.0 
                    for task in ['tx_index', 'log_index', 'snapshot']
                ):
                    status['stage'] = 'fully_synced'
                    status['chain_progress'] = 100.0
                    status['state_progress'] = 100.0
                    status['snapshot_progress'] = 100.0
                    status['tx_index_progress'] = 100.0
                    status['log_index_progress'] = 100.0
                continue
        
        # Calculate progress estimates
        if status['stage'] == 'state_healing' and status['state_nodes'] > 0:
            estimated_total_nodes = 3000000
            status['state_progress'] = min(95.0, (status['state_nodes'] / estimated_total_nodes) * 100)
        
        if status['snapshot_accounts'] > 0:
            # Rough estimate - mainnet has ~340M accounts
            estimated_total_accounts = 340000000
            status['snapshot_progress'] = min(95.0, (status['snapshot_accounts'] / estimated_total_accounts) * 100)
        
        return status
    
    def parse_lighthouse_status(self, logs: List[str]) -> Dict:
        """Parse Lighthouse consensus client status from logs"""
        status = {
            'stage': 'unknown',
            'sync_state': 'unknown',
            'peers': 0,
            'distance_slots': 0,
            'speed_slots_sec': 0.0,
            'current_slot': None,
            'finalized_epoch': None,
            'is_optimistic': False,
            'engine_errors': 0,
            'last_error': None,
            'restart_count': 0,
            'last_activity': None,
            'historical_blocks_remaining': 0,
            'is_synced': False,
            'head_block_hash': None
        }
        
        engine_error_count = 0
        
        for line in logs:
            # Sync state updates
            if 'Sync state updated' in line:
                match = re.search(r'new_state: ([^,]+)', line)
                if match:
                    status['sync_state'] = match.group(1).strip()
                    status['last_activity'] = self._extract_timestamp(line)
            
            # Syncing progress
            sync_match = re.search(r'Syncing.*peers: "(\d+)".*distance: "(\d+) slots.*speed: "([0-9.]+) slots/sec"', line)
            if sync_match:
                status['stage'] = 'syncing'
                status['peers'] = int(sync_match.group(1))
                status['distance_slots'] = int(sync_match.group(2))
                status['speed_slots_sec'] = float(sync_match.group(3))
                status['last_activity'] = self._extract_timestamp(line)
            
            # Current status
            if 'Synced' in line and 'slot:' in line:
                slot_match = re.search(r'slot: (\d+)', line)
                epoch_match = re.search(r'finalized_epoch: (\d+)', line)
                if slot_match:
                    status['current_slot'] = int(slot_match.group(1))
                if epoch_match:
                    status['finalized_epoch'] = int(epoch_match.group(1))
                status['stage'] = 'synced'
                status['last_activity'] = self._extract_timestamp(line)
            
            # Optimistic head warnings
            if 'Head is optimistic' in line:
                status['is_optimistic'] = True
            
            # Engine API errors
            if 'Execution engine call failed' in line or 'Error during execution engine upcheck' in line:
                engine_error_count += 1
                if 'timeout' in line.lower():
                    status['last_error'] = 'engine_timeout'
                elif 'Invalid parameters' in line:
                    status['last_error'] = 'invalid_parameters'
            
            # Database errors
            if 'Database write failed' in line:
                status['last_error'] = 'database_write_failed'
        
        status['engine_errors'] = engine_error_count
        return status
    
    def _extract_timestamp(self, log_line: str) -> Optional[datetime]:
        """Extract timestamp from log line"""
        # Match format: Aug 29 18:30:25.965
        match = re.search(r'(\w{3} \d{2} \d{2}:\d{2}:\d{2})', log_line)
        if match:
            try:
                # Assume current year
                time_str = f"2025 {match.group(1)}"
                return datetime.strptime(time_str, "%Y %b %d %H:%M:%S")
            except:
                pass
        return None
    
    def get_pod_info(self) -> Dict:
        """Get pod status information"""
        try:
            # Get pod list
            cmd = "kubectl get pods -n devbox -o json"
            result = subprocess.run(cmd.split(), capture_output=True, text=True, timeout=10)
            if result.returncode != 0:
                return {}
                
            pods_data = json.loads(result.stdout)
            pod_info = {}
            
            for pod in pods_data.get('items', []):
                name = pod['metadata']['name']
                if 'eth-mainnet-01' in name:
                    status = pod['status']
                    pod_info[name] = {
                        'phase': status.get('phase', 'Unknown'),
                        'ready': False,
                        'restarts': 0,
                        'age_hours': 0
                    }
                    
                    # Calculate age
                    start_time = pod['status'].get('startTime')
                    if start_time:
                        start = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                        age = datetime.now(start.tzinfo) - start
                        pod_info[name]['age_hours'] = age.total_seconds() / 3600
                    
                    # Check container statuses
                    for container_status in status.get('containerStatuses', []):
                        if container_status.get('ready', False):
                            pod_info[name]['ready'] = True
                        pod_info[name]['restarts'] = container_status.get('restartCount', 0)
            
            return pod_info
        except Exception:
            return {}
    
    def analyze_issues(self, geth_status: Dict, lighthouse_status: Dict, pod_info: Dict) -> List[str]:
        """Analyze current issues and provide recommendations"""
        issues = []
        
        # Check for high restart counts
        for pod_name, info in pod_info.items():
            if info['restarts'] > 10:
                issues.append(f"🔄 {pod_name.split('-')[2]} restarted {info['restarts']} times - check resource limits")
        
        # Check Geth state healing
        if geth_status['stage'] == 'state_healing':
            if geth_status['eta_minutes']:
                issues.append(f"⏳ Geth in state healing (~{geth_status['eta_minutes']:.0f}min remaining)")
            else:
                issues.append("⏳ Geth in state healing phase - RPC limited until complete")
        
        # Check Lighthouse engine errors
        if lighthouse_status['engine_errors'] > 5:
            if lighthouse_status['last_error'] == 'engine_timeout':
                issues.append("🔌 Lighthouse cannot connect to Geth Engine API (timeouts)")
            elif lighthouse_status['last_error'] == 'invalid_parameters':
                issues.append("⚠️ Lighthouse receiving invalid parameter errors from Geth")
            else:
                issues.append(f"❌ Lighthouse has {lighthouse_status['engine_errors']} engine errors")
        
        # Check optimistic head
        if lighthouse_status['is_optimistic']:
            issues.append("🔍 Chain head is optimistic (unverified by execution layer)")
        
        # Check sync stages
        if geth_status['stage'] == 'chain_download' and geth_status['chain_progress'] < 100:
            issues.append(f"📥 Geth downloading chain ({geth_status['chain_progress']:.1f}% complete)")
        
        if lighthouse_status['sync_state'] == 'Syncing Historical Blocks':
            issues.append("📜 Lighthouse syncing historical consensus data")
        
        return issues
    
    def format_sync_bar(self, percentage: float, width: int = 30, style: str = '█') -> str:
        """Create a visual progress bar"""
        filled = int(width * percentage / 100)
        empty = width - filled
        return f"[{style * filled}{'░' * empty}]"
    
    def clear_screen(self):
        """Clear terminal screen"""
        os.system('clear')
    
    def monitor(self):
        """Main monitoring loop"""
        print("🚀 Starting Advanced Ethereum Node Monitor...")
        print(f"📍 Target Block: {self.target_block:,}")
        print("=" * 80)
        
        while True:
            try:
                # Get Django node info
                eth_chain = Chain.objects.get(chain_id=1)
                node = Node.objects.filter(chain=eth_chain).first()
                
                if not node:
                    print("❌ No Ethereum node found in database")
                    time.sleep(10)
                    continue
                
                # Get Kubernetes pod info
                pod_info = self.get_pod_info()
                
                # Find pod names
                execution_pod = next((name for name in pod_info.keys() if 'execution' in name), None)
                consensus_pod = next((name for name in pod_info.keys() if 'consensus' in name), None)
                
                # Get logs and parse status
                geth_status = {}
                lighthouse_status = {}
                
                if execution_pod:
                    geth_logs = self.get_pod_logs(execution_pod, 'geth', 30)
                    geth_status = self.parse_geth_sync_status(geth_logs)
                    
                    # Also check RPC for more accurate sync status
                    rpc_sync = self.check_geth_rpc_sync()
                    if rpc_sync['fully_synced']:
                        geth_status['stage'] = 'fully_synced'
                        geth_status['is_fully_synced'] = True
                    elif rpc_sync.get('tx_index_remaining', 0) > 0:
                        geth_status['tx_remaining'] = rpc_sync['tx_index_remaining']
                        if 'tx_indexing' not in geth_status['substages']:
                            geth_status['substages'].append('tx_indexing')
                
                if consensus_pod:
                    lighthouse_logs = self.get_pod_logs(consensus_pod, 'lighthouse-beacon', 50)
                    lighthouse_status = self.parse_lighthouse_status(lighthouse_logs)
                
                # Calculate sync speed for Django node
                current_time = datetime.now()
                blocks_per_second = 0
                if self.prev_block and self.prev_time and node.current_block_height:
                    time_diff = (current_time - self.prev_time).total_seconds()
                    if time_diff > 0:
                        block_diff = node.current_block_height - self.prev_block
                        instant_speed = block_diff / time_diff if time_diff > 0 else 0
                        self.speed_history.append(instant_speed)
                        if len(self.speed_history) > 6:
                            self.speed_history.pop(0)
                        blocks_per_second = sum(self.speed_history) / len(self.speed_history) if self.speed_history else 0
                
                # Analyze issues
                issues = self.analyze_issues(geth_status, lighthouse_status, pod_info)
                
                # Display status
                self.clear_screen()
                print("=" * 90)
                print("              🔗 ADVANCED ETHEREUM L1 NODE MONITOR 🔗")
                print("=" * 90)
                print()
                
                # Node overview
                print("📦 NODE OVERVIEW")
                print("-" * 45)
                print(f"Node: {node.name}")
                print(f"Clients: {node.execution_client} + {node.consensus_client}")
                print(f"Status: {node.status.upper()}")
                print(f"RPC: {node.execution_rpc_url}")
                print()
                
                # Kubernetes Pods Status
                print("☸️  KUBERNETES PODS")
                print("-" * 45)
                for pod_name, info in pod_info.items():
                    client_type = "Execution" if "execution" in pod_name else "Consensus"
                    ready_icon = "✅" if info['ready'] else "❌"
                    restart_info = f"({info['restarts']} restarts)" if info['restarts'] > 0 else ""
                    print(f"{ready_icon} {client_type}: {info['phase']} {restart_info} - {info['age_hours']:.1f}h old")
                print()
                
                # Execution Layer (Geth) Status
                print("⚙️  EXECUTION LAYER (GETH)")
                print("-" * 45)
                
                if geth_status:
                    stage_icons = {
                        'chain_download': '📥',
                        'state_healing': '🔄',
                        'post_healing': '🔧',
                        'fully_synced': '✅',
                        'unknown': '❓'
                    }
                    
                    icon = stage_icons.get(geth_status['stage'], '❓')
                    stage_name = geth_status['stage'].replace('_', ' ').title()
                    if geth_status['stage'] == 'post_healing':
                        stage_name = f"Post-Healing ({len(geth_status['substages'])} processes)"
                    print(f"{icon} Stage: {stage_name}")
                    
                    # Show chain download
                    if geth_status['chain_progress'] > 0:
                        chain_bar = self.format_sync_bar(geth_status['chain_progress'])
                        print(f"   Chain: {chain_bar} {geth_status['chain_progress']:.2f}%")
                    
                    # Show state healing
                    if geth_status['state_progress'] > 0 and 'state_healing' in geth_status['substages']:
                        state_bar = self.format_sync_bar(geth_status['state_progress'])
                        print(f"   State Healing: {state_bar} {geth_status['state_progress']:.1f}%")
                        print(f"     Accounts: {geth_status['state_accounts']:,}")
                        print(f"     Nodes: {geth_status['state_nodes']:,}")
                        print(f"     Pending: {geth_status['pending_state']:,}")
                    
                    # Show snapshot generation
                    if 'snapshot_generation' in geth_status['substages']:
                        if geth_status['snapshot_progress'] > 0:
                            snap_bar = self.format_sync_bar(geth_status['snapshot_progress'])
                            print(f"   Snapshot: {snap_bar} {geth_status['snapshot_progress']:.1f}%")
                        print(f"     Accounts: {geth_status['snapshot_accounts']:,}")
                        print(f"     Slots: {geth_status['snapshot_slots']:,}")
                    
                    # Show transaction indexing
                    if 'tx_indexing' in geth_status['substages']:
                        if geth_status['tx_index_progress'] > 0:
                            tx_bar = self.format_sync_bar(geth_status['tx_index_progress'])
                            print(f"   TX Index: {tx_bar} {geth_status['tx_index_progress']:.1f}%")
                        print(f"     Blocks: {geth_status['tx_blocks_processed']:,}")
                        print(f"     Remaining: {geth_status['tx_remaining']:,}")
                    
                    # Show log indexing
                    if 'log_indexing' in geth_status['substages']:
                        if geth_status['log_index_progress'] > 0:
                            log_bar = self.format_sync_bar(geth_status['log_index_progress'])
                            print(f"   Log Index: {log_bar} {geth_status['log_index_progress']:.1f}%")
                        print(f"     Processed: {geth_status['log_blocks_processed']:,}")
                        print(f"     Remaining: {geth_status['log_remaining']:,}")
                    
                    # Show current block
                    if geth_status['current_block']:
                        print(f"   Current Block: {geth_status['current_block']:,}")
                    
                    # Show ETA
                    if geth_status['eta_minutes']:
                        eta_hours = int(geth_status['eta_minutes'] // 60)
                        eta_mins = int(geth_status['eta_minutes'] % 60)
                        if eta_hours > 0:
                            print(f"   ETA: {eta_hours}h {eta_mins}m")
                        else:
                            print(f"   ETA: {eta_mins}m")
                
                else:
                    print("❓ Unable to determine Geth status")
                print()
                
                # Consensus Layer (Lighthouse) Status  
                print("🔮 CONSENSUS LAYER (LIGHTHOUSE)")
                print("-" * 45)
                
                if lighthouse_status:
                    stage_icons = {
                        'syncing': '🔄',
                        'synced': '✅',
                        'unknown': '❓'
                    }
                    
                    icon = stage_icons.get(lighthouse_status['stage'], '❓')
                    print(f"{icon} Stage: {lighthouse_status['sync_state']}")
                    print(f"   Peers: {lighthouse_status['peers']}")
                    
                    if lighthouse_status['current_slot']:
                        print(f"   Current Slot: {lighthouse_status['current_slot']:,}")
                    
                    if lighthouse_status['finalized_epoch']:
                        print(f"   Finalized Epoch: {lighthouse_status['finalized_epoch']:,}")
                    
                    if lighthouse_status['distance_slots'] > 0:
                        print(f"   Behind: {lighthouse_status['distance_slots']} slots")
                    
                    if lighthouse_status['speed_slots_sec'] > 0:
                        print(f"   Speed: {lighthouse_status['speed_slots_sec']:.2f} slots/sec")
                    
                    if lighthouse_status['is_optimistic']:
                        print("   ⚠️ Head is optimistic (unverified)")
                    
                    if lighthouse_status['engine_errors'] > 0:
                        print(f"   ❌ Engine errors: {lighthouse_status['engine_errors']}")
                
                else:
                    print("❓ Unable to determine Lighthouse status")
                print()
                
                # Django DB Status
                print("🗄️  DATABASE STATUS")
                print("-" * 45)
                django_bar = self.format_sync_bar(node.execution_sync_progress)
                print(f"   Execution: {django_bar} {node.execution_sync_progress:.2f}%")
                
                consensus_bar = self.format_sync_bar(node.consensus_sync_progress)  
                print(f"   Consensus: {consensus_bar} {node.consensus_sync_progress:.2f}%")
                
                if node.current_block_height:
                    print(f"   Current Block: {node.current_block_height:,}")
                    blocks_behind = self.target_block - node.current_block_height
                    if blocks_behind > 0:
                        print(f"   Target Block: {self.target_block:,}")
                        print(f"   Blocks Behind: {blocks_behind:,}")
                        
                        if blocks_per_second > 0:
                            eta_seconds = blocks_behind / blocks_per_second
                            if eta_seconds < 3600:
                                print(f"   ETA to Target: {int(eta_seconds/60)}m")
                            else:
                                print(f"   ETA to Target: {int(eta_seconds/3600)}h {int((eta_seconds%3600)/60)}m")
                    else:
                        print(f"   ✅ PAST TARGET BLOCK!")
                
                if node.last_health_check:
                    time_since = datetime.now(node.last_health_check.tzinfo) - node.last_health_check
                    print(f"   Last Update: {int(time_since.total_seconds())}s ago")
                print()
                
                # Issues and Recommendations
                if issues:
                    print("⚠️  CURRENT ISSUES")
                    print("-" * 45)
                    for issue in issues[:5]:  # Limit to top 5 issues
                        print(f"   {issue}")
                    print()
                
                # Overall Status
                print("📈 OVERALL STATUS")
                print("-" * 45)
                
                # Determine overall health state
                geth_fully_synced = geth_status.get('is_fully_synced', False) or geth_status.get('stage') == 'fully_synced'
                lighthouse_synced = lighthouse_status.get('is_synced', False) or not lighthouse_status.get('is_optimistic', True)
                tx_indexing_complete = geth_status.get('tx_remaining', 0) == 0
                
                # Health indicators
                health_checks = {
                    'Execution synced': geth_fully_synced,
                    'Consensus synced': lighthouse_synced,
                    'TX indexing complete': tx_indexing_complete,
                    'No pod restarts': all(info['restarts'] < 5 for info in pod_info.values()),
                    'No engine errors': lighthouse_status.get('engine_errors', 0) == 0
                }
                
                # Count passed checks
                passed_checks = sum(1 for check in health_checks.values() if check)
                total_checks = len(health_checks)
                
                # Display health status
                if passed_checks == total_checks:
                    print("🟢 FULLY HEALTHY - All systems operational!")
                    print("   ✅ Ready for production use")
                elif passed_checks >= 3:
                    print("🟡 PARTIALLY HEALTHY - Some issues present")
                    for check_name, passed in health_checks.items():
                        if not passed:
                            print(f"   ❌ {check_name}")
                else:
                    print("🔴 UNHEALTHY - Major sync in progress")
                    print(f"   Health score: {passed_checks}/{total_checks}")
                
                # Show specific status details
                if geth_status.get('stage') == 'state_healing':
                    print("   ⏳ State healing in progress")
                elif geth_status.get('tx_remaining', 0) > 0:
                    print(f"   📊 TX indexing: {geth_status.get('tx_remaining', 0):,} blocks remaining")
                elif lighthouse_status.get('is_optimistic'):
                    print("   ⚠️ Consensus running in optimistic mode")
                
                print()
                print("-" * 90)
                uptime = datetime.now() - self.startup_time
                print(f"Monitor uptime: {int(uptime.total_seconds())}s | Last updated: {current_time.strftime('%H:%M:%S')} | Press Ctrl+C to exit")
                
                # Update tracking variables
                if node.current_block_height:
                    self.prev_block = node.current_block_height
                    self.prev_time = current_time
                
                time.sleep(15)  # Update every 15 seconds
                
            except KeyboardInterrupt:
                print("\n\n👋 Exiting advanced monitor...")
                break
            except Exception as e:
                print(f"\n❌ Monitor error: {e}")
                time.sleep(10)

def main():
    monitor = EthereumNodeMonitor()
    monitor.monitor()

if __name__ == "__main__":
    main()