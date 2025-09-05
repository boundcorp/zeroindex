#!/usr/bin/env python3
"""
Ethereum Node Sync Monitor
Displays real-time sync progress for our Ethereum L1 node
"""

import os
import sys
import time
import django
from datetime import datetime, timedelta

# Setup Django
sys.path.append('/home/dev/p/boundcorp/zeroindex')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'zeroindex.settings.project')
django.setup()

from zeroindex.apps.nodes.models import Node
from zeroindex.apps.chains.models import Chain

def format_number(n):
    """Format large numbers with commas"""
    if n is None:
        return "N/A"
    return f"{n:,}"

def estimate_time_remaining(current_block, target_block, blocks_per_second):
    """Estimate time remaining based on sync speed"""
    if blocks_per_second <= 0:
        return "Unknown"
    
    blocks_remaining = target_block - current_block
    seconds_remaining = blocks_remaining / blocks_per_second
    
    if seconds_remaining < 60:
        return f"{int(seconds_remaining)}s"
    elif seconds_remaining < 3600:
        return f"{int(seconds_remaining/60)}m {int(seconds_remaining%60)}s"
    elif seconds_remaining < 86400:
        hours = int(seconds_remaining/3600)
        mins = int((seconds_remaining%3600)/60)
        return f"{hours}h {mins}m"
    else:
        days = int(seconds_remaining/86400)
        hours = int((seconds_remaining%86400)/3600)
        return f"{days}d {hours}h"

def clear_screen():
    """Clear terminal screen"""
    os.system('clear')

def main():
    # Target block we need for chunk repair
    TARGET_BLOCK = 23242692
    
    # Get Ethereum mainnet chain
    try:
        eth_chain = Chain.objects.get(chain_id=1)
    except Chain.DoesNotExist:
        print("❌ Ethereum chain not found in database")
        sys.exit(1)
    
    # Track previous block for calculating sync speed
    prev_block = None
    prev_time = None
    blocks_per_second = 0
    speed_history = []  # Keep last 6 measurements for averaging (1 minute)
    
    print("🔄 Starting Ethereum Node Sync Monitor...")
    print(f"📍 Target Block: {format_number(TARGET_BLOCK)}")
    print("-" * 80)
    
    while True:
        try:
            # Get the node
            node = Node.objects.filter(chain=eth_chain).first()
            
            if not node:
                clear_screen()
                print("❌ No Ethereum node found")
                time.sleep(10)
                continue
            
            # Calculate sync speed
            current_time = datetime.now()
            if prev_block and prev_time and node.current_block_height:
                time_diff = (current_time - prev_time).total_seconds()
                if time_diff > 0:
                    block_diff = node.current_block_height - prev_block
                    instant_speed = block_diff / time_diff
                    
                    # Add to history and keep only last 6 measurements
                    speed_history.append(instant_speed)
                    if len(speed_history) > 6:
                        speed_history.pop(0)
                    
                    # Use average for more stable ETA
                    blocks_per_second = sum(speed_history) / len(speed_history)
            
            # Clear screen and display status
            clear_screen()
            print("=" * 80)
            print("              🔗 ETHEREUM L1 NODE SYNC MONITOR 🔗")
            print("=" * 80)
            print()
            
            # Node info
            print(f"📦 Node: {node.name}")
            print(f"🔧 Clients: {node.execution_client} (execution) + {node.consensus_client} (consensus)")
            print(f"🔌 RPC: {node.execution_rpc_url}")
            print(f"📊 Status: {node.status.upper()}")
            print()
            
            # Sync progress
            print("SYNC PROGRESS")
            print("-" * 40)
            
            # Execution layer
            exec_bar_width = 30
            exec_filled = int(exec_bar_width * node.execution_sync_progress / 100)
            exec_bar = "█" * exec_filled + "░" * (exec_bar_width - exec_filled)
            print(f"⚙️  Execution: [{exec_bar}] {node.execution_sync_progress:.2f}%")
            
            if node.current_block_height:
                print(f"   Current Block: {format_number(node.current_block_height)}")
                print(f"   Target Block:  {format_number(TARGET_BLOCK)}")
                blocks_behind = TARGET_BLOCK - node.current_block_height
                
                if blocks_behind > 0:
                    print(f"   Blocks Behind: {format_number(blocks_behind)}")
                    
                    if blocks_per_second > 0:
                        print(f"   Sync Speed:    {blocks_per_second:.1f} blocks/sec (avg)")
                        eta = estimate_time_remaining(node.current_block_height, TARGET_BLOCK, blocks_per_second)
                        print(f"   ETA to Target: {eta}")
                    else:
                        print(f"   Sync Speed:    Calculating... ({len(speed_history)} samples)")
                        if prev_block and prev_block == node.current_block_height:
                            print(f"   ETA to Target: Node appears stalled")
                        else:
                            print(f"   ETA to Target: Waiting for speed data...")
                else:
                    print(f"   ✅ SYNCED PAST TARGET BLOCK!")
            print()
            
            # Consensus layer
            cons_bar_width = 30
            cons_filled = int(cons_bar_width * node.consensus_sync_progress / 100)
            cons_bar = "█" * cons_filled + "░" * (cons_bar_width - cons_filled)
            print(f"🔮 Consensus: [{cons_bar}] {node.consensus_sync_progress:.2f}%")
            
            if node.consensus_head_slot:
                print(f"   Head Slot: {format_number(node.consensus_head_slot)}")
            print()
            
            # Overall status
            print("OVERALL STATUS")
            print("-" * 40)
            overall = node.overall_sync_progress
            overall_bar_width = 30
            overall_filled = int(overall_bar_width * overall / 100)
            overall_bar = "█" * overall_filled + "░" * (overall_bar_width - overall_filled)
            print(f"📈 Overall:  [{overall_bar}] {overall:.2f}%")
            
            if node.is_fully_synced:
                print("✅ Node is FULLY SYNCED!")
            elif node.current_block_height and node.current_block_height >= TARGET_BLOCK:
                print("✅ Node has reached target block for chunk repair!")
            else:
                print("⏳ Node is still syncing...")
                
                # Calculate ETA to 100% sync (assuming linear progress)
                if blocks_per_second > 0 and node.current_block_height:
                    # Estimate current chain tip (roughly 20M blocks as of 2025)
                    estimated_chain_tip = 21000000  # Adjust based on current Ethereum height
                    blocks_to_tip = estimated_chain_tip - node.current_block_height
                    if blocks_to_tip > 0:
                        eta_full = estimate_time_remaining(node.current_block_height, estimated_chain_tip, blocks_per_second)
                        print(f"⏱️  ETA to ~100% sync: {eta_full}")
            
            print()
            print("-" * 80)
            print(f"Last Updated: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
            print("Press Ctrl+C to exit")
            
            # Update tracking variables
            if node.current_block_height:
                prev_block = node.current_block_height
                prev_time = current_time
            
            # Wait before next update
            time.sleep(10)
            
        except KeyboardInterrupt:
            print("\n\n👋 Exiting sync monitor...")
            break
        except Exception as e:
            print(f"\n❌ Error: {e}")
            time.sleep(10)

if __name__ == "__main__":
    main()