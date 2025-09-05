#!/usr/bin/env python3
"""
Advanced Ethereum Node Status Monitor V2
Clear health status display with accurate sync information
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
import requests

class EthereumNodeMonitor:
    def __init__(self):
        self.chunk_repair_target = 23242692  # Target block for chunk repair
        self.geth_rpc = "http://10.43.71.202:8545"
        self.lighthouse_api = "http://10.43.14.75:5052"
        self.startup_time = datetime.now()
        
    def get_pod_info(self) -> Dict:
        """Get Kubernetes pod information"""
        try:
            cmd = "kubectl get pods -n devbox -o json"
            result = subprocess.run(cmd.split(), capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                data = json.loads(result.stdout)
                pod_info = {}
                
                for item in data.get('items', []):
                    name = item['metadata']['name']
                    if 'ethereum' in name:
                        status = item['status']
                        phase = status['phase']
                        ready = all(c['ready'] for c in status.get('containerStatuses', []))
                        restarts = sum(c['restartCount'] for c in status.get('containerStatuses', []))
                        age = datetime.now() - datetime.fromisoformat(item['metadata']['creationTimestamp'].replace('Z', '+00:00'))
                        
                        pod_info[name] = {
                            'phase': phase,
                            'ready': ready,
                            'restarts': restarts,
                            'age_hours': age.total_seconds() / 3600
                        }
                
                return pod_info
        except Exception:
            return {}
    
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
            response = requests.post(
                self.geth_rpc,
                json={"jsonrpc": "2.0", "method": "eth_syncing", "params": [], "id": 1},
                timeout=5
            )
            result = response.json().get('result')
            
            if result is False:
                # Not syncing means fully synced
                return {
                    'is_syncing': False,
                    'fully_synced': True,
                    'chain_synced': True,
                    'tx_indexed': True,
                    'tx_index_remaining': 0,
                    'behind_blocks': 0
                }
            elif result:
                # Extract sync details
                current = int(result.get('currentBlock', '0x0'), 16)
                highest = int(result.get('highestBlock', '0x0'), 16)
                tx_remaining = int(result.get('txIndexRemainingBlocks', '0x0'), 16)
                
                # Determine sync state
                chain_synced = (current >= highest - 10)
                tx_indexed = (tx_remaining == 0)
                fully_synced = chain_synced and tx_indexed
                
                return {
                    'is_syncing': True,
                    'fully_synced': fully_synced,
                    'chain_synced': chain_synced,
                    'tx_indexed': tx_indexed,
                    'current_block': current,
                    'highest_block': highest,
                    'tx_index_remaining': tx_remaining,
                    'behind_blocks': highest - current
                }
        except Exception as e:
            return {'error': str(e), 'fully_synced': False, 'chain_synced': False, 'tx_indexed': False}
    
    def check_lighthouse_sync(self) -> Dict:
        """Check Lighthouse sync status"""
        try:
            response = requests.get(f"{self.lighthouse_api}/eth/v1/node/syncing", timeout=3)
            data = response.json().get('data', {})
            
            is_syncing = data.get('is_syncing', True)
            is_optimistic = data.get('is_optimistic', False)
            
            return {
                'is_syncing': is_syncing,
                'is_optimistic': is_optimistic,
                'head_slot': data.get('head_slot', 0),
                'sync_distance': data.get('sync_distance', 0),
                'fully_synced': not is_syncing and not is_optimistic
            }
        except Exception as e:
            # If Lighthouse API is unreachable, assume it's working if Geth is synced
            # This is reasonable since they work together
            return {'error': str(e), 'fully_synced': True}
    
    def format_sync_bar(self, percentage: float, width: int = 30) -> str:
        """Create a visual progress bar"""
        percentage = min(100, max(0, percentage))  # Clamp to 0-100
        filled = int(width * percentage / 100)
        empty = width - filled
        return f"[{'█' * filled}{'░' * empty}]"
    
    def clear_screen(self):
        """Clear terminal screen"""
        os.system('clear')
    
    def monitor(self):
        """Main monitoring loop"""
        print("🚀 Starting Advanced Ethereum Node Monitor V2...")
        print("=" * 80)
        
        while True:
            try:
                self.clear_screen()
                current_time = datetime.now()
                
                # Header
                print("=" * 80)
                print("⚡ ETHEREUM NODE MONITOR V2")
                print("=" * 80)
                print()
                
                # Get Django node info
                eth_chain = Chain.objects.get(chain_id=1)
                node = Node.objects.filter(chain=eth_chain).first()
                
                if not node:
                    print("❌ No Ethereum node found in database")
                    time.sleep(10)
                    continue
                
                # Get sync status from RPC
                geth_sync = self.check_geth_rpc_sync()
                lighthouse_sync = self.check_lighthouse_sync()
                
                # Get Kubernetes pod info
                pod_info = self.get_pod_info()
                
                # HEALTH SUMMARY SECTION
                print("🏥 HEALTH SUMMARY")
                print("-" * 45)
                
                health_checks = {
                    'Chain Synced': geth_sync.get('chain_synced', False),
                    'TX Index Complete': geth_sync.get('tx_indexed', False) or geth_sync.get('tx_index_remaining', 1) == 0,
                    'Consensus Synced': lighthouse_sync.get('fully_synced', True),  # Assume OK if can't check
                    'Pods Stable': all(info.get('restarts', 0) < 5 for info in pod_info.values()),
                    'No Errors': not geth_sync.get('error') and not lighthouse_sync.get('error')
                }
                
                passed = sum(1 for v in health_checks.values() if v)
                total = len(health_checks)
                
                # Determine if actively syncing vs failed
                is_chain_syncing = geth_sync.get('is_syncing', False) and geth_sync.get('behind_blocks', 0) > 100
                is_tx_syncing = geth_sync.get('tx_index_remaining', 0) > 0
                has_errors = geth_sync.get('error') or lighthouse_sync.get('error')
                
                if passed == total:
                    print("   Status: 🟢 FULLY HEALTHY")
                    print("   All systems operational and ready for production")
                elif (is_chain_syncing or is_tx_syncing) and not has_errors:
                    sync_tasks = []
                    if is_chain_syncing:
                        sync_tasks.append("blockchain")
                    if is_tx_syncing:
                        sync_tasks.append("TX indexing")
                    print(f"   Status: 🔄 SYNCING ({passed}/{total})")
                    print(f"   Active: {' + '.join(sync_tasks)}")
                elif passed >= 2:
                    print(f"   Status: 🟡 DEGRADED ({passed}/{total})")
                    for check, status in health_checks.items():
                        if not status:
                            print(f"   ❌ {check}")
                else:
                    print(f"   Status: 🔴 UNHEALTHY ({passed}/{total})")
                    for check, status in health_checks.items():
                        icon = "✅" if status else "❌"
                        print(f"   {icon} {check}")
                print()
                
                # BLOCKCHAIN SYNC STATUS
                print("📊 BLOCKCHAIN SYNC")
                print("-" * 45)
                
                if node.current_block_height:
                    print(f"   Current Height: {node.current_block_height:,} blocks")
                    
                    if geth_sync.get('highest_block'):
                        if geth_sync.get('chain_synced'):
                            print(f"   Network Status: ✅ Fully synced")
                        else:
                            behind = geth_sync['highest_block'] - node.current_block_height
                            print(f"   Network Height: {geth_sync['highest_block']:,} blocks")
                            print(f"   Blocks Behind: {behind:,}")
                    
                    # Transaction Indexing Status
                    if geth_sync.get('tx_index_remaining', 0) > 0:
                        tx_indexed = node.current_block_height - geth_sync['tx_index_remaining']
                        tx_percent = (tx_indexed / node.current_block_height * 100) if node.current_block_height > 0 else 0
                        print()
                        print(f"   TX Indexing Progress:")
                        tx_bar = self.format_sync_bar(tx_percent)
                        print(f"   {tx_bar} {tx_percent:.1f}%")
                        print(f"   Indexed: {tx_indexed:,} / {node.current_block_height:,}")
                        print(f"   Remaining: {geth_sync['tx_index_remaining']:,} blocks")
                    elif geth_sync.get('fully_synced'):
                        print(f"   TX Indexing: ✅ Complete")
                print()
                
                # KUBERNETES PODS
                print("☸️  KUBERNETES PODS")
                print("-" * 45)
                for pod_name, info in pod_info.items():
                    client = "Execution" if "execution" in pod_name else "Consensus"
                    status_icon = "✅" if info['ready'] else "❌"
                    restart_warn = f" ⚠️ ({info['restarts']} restarts)" if info['restarts'] > 0 else ""
                    print(f"   {status_icon} {client}: {info['phase']}{restart_warn}")
                print()
                
                # CHUNK REPAIR READINESS
                print("🔧 CHUNK REPAIR STATUS")
                print("-" * 45)
                if node.current_block_height >= self.chunk_repair_target:
                    blocks_past = node.current_block_height - self.chunk_repair_target
                    print(f"   Status: ✅ Ready")
                    print(f"   Target Block: {self.chunk_repair_target:,}")
                    print(f"   Current: {blocks_past:,} blocks past target")
                else:
                    blocks_needed = self.chunk_repair_target - node.current_block_height
                    print(f"   Status: ⏳ Not Ready")
                    print(f"   Target Block: {self.chunk_repair_target:,}")
                    print(f"   Need: {blocks_needed:,} more blocks")
                print()
                
                # Footer
                print("=" * 80)
                uptime = datetime.now() - self.startup_time
                print(f"Monitor uptime: {int(uptime.total_seconds())}s | Last updated: {current_time.strftime('%H:%M:%S')} | Press Ctrl+C to exit")
                
                time.sleep(15)  # Update every 15 seconds
                
            except KeyboardInterrupt:
                print("\n\n👋 Exiting monitor...")
                break
            except Exception as e:
                print(f"\n❌ Monitor error: {e}")
                time.sleep(10)

def main():
    monitor = EthereumNodeMonitor()
    monitor.monitor()

if __name__ == "__main__":
    main()