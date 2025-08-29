#!/usr/bin/env python3
"""
Live testing script - actually run the blocks app with real data.
Proof of concept with public RPC.
"""

import asyncio
import sys
import os
from pathlib import Path
import json
from datetime import datetime

# Add the apps/blocks directory to the path
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

# Set PYTHONPATH
os.environ['PYTHONPATH'] = str(current_dir) + ':' + os.environ.get('PYTHONPATH', '')

from trace_verifier import TraceVerifier
from block_chunker import BlockChunker
from test_chunks import ChunkValidator


async def test_trace_verification():
    """Test trace verification with public RPC."""
    print("🔍 Testing trace verification...")
    
    # Use a public RPC endpoint (limited but should work for basic testing)
    public_rpc = "https://ethereum.publicnode.com"
    
    verifier = TraceVerifier(public_rpc)
    
    print(f"Connecting to: {public_rpc}")
    
    if not verifier.check_connection():
        print("❌ Cannot connect to RPC")
        return False
        
    latest = verifier.get_latest_block()
    print(f"✅ Connected - Latest block: {latest}")
    
    # Test basic capability (public RPCs usually don't have debug/trace APIs)
    print("\n🧪 Testing trace capabilities...")
    results = verifier.verify_full_trace_capability(sample_blocks=1)
    
    print(f"Node capabilities:")
    for method, available in results.get('capabilities', {}).items():
        status = "✅" if available else "❌"
        print(f"  {status} {method}")
        
    print(f"\n💡 Recommendation: {results.get('recommendation')}")
    
    return True


async def test_block_chunking():
    """Test block chunking with real data."""
    print("\n📦 Testing block chunking...")
    
    # Use public RPC 
    public_rpc = "https://ethereum.publicnode.com"
    
    chunker = BlockChunker(public_rpc, './test_data/blocks')
    
    if not chunker.w3.is_connected():
        print("❌ Cannot connect to blockchain")
        return False
        
    latest = chunker.w3.eth.block_number
    print(f"Latest block: {latest}")
    
    # Test with just 2 recent blocks to keep it small
    start_block = latest - 2
    end_block = latest - 1
    
    print(f"Creating test chunk for blocks {start_block}-{end_block}...")
    
    try:
        # Get block data (without traces since public RPC won't have them)
        chunk_data = chunker.create_chunk(start_block, end_block)
        
        print(f"✅ Created chunk with {len(chunk_data['blocks'])} blocks")
        print(f"Total transactions: {chunk_data['metadata']['total_transactions']}")
        
        # Save and compress
        filename = chunker.save_chunk(chunk_data)
        print(f"💾 Saved as: {filename}")
        
        # Check file size
        chunk_file = chunker.chunks_dir / filename
        file_size = chunk_file.stat().st_size
        print(f"📏 Compressed size: {file_size:,} bytes ({file_size/1024:.2f} KB)")
        
        return chunk_file, chunk_data
        
    except Exception as e:
        print(f"❌ Error creating chunk: {e}")
        return None, None


async def test_validation(chunk_file, chunk_data):
    """Test chunk validation."""
    if not chunk_file:
        return False
        
    print(f"\n🧪 Testing validation of {chunk_file.name}...")
    
    public_rpc = "https://ethereum.publicnode.com"
    validator = ChunkValidator(public_rpc)
    
    # Run validation
    result = validator.run_full_validation(chunk_file)
    
    print(f"Validation result: {'✅ PASSED' if result['success'] else '❌ FAILED'}")
    
    for test in result['tests']:
        status_map = {
            'PASS': '✅',
            'FAIL': '❌', 
            'PARTIAL': '⚠️',
            'ERROR': '🚫'
        }
        status = status_map.get(test['result'], '❓')
        print(f"  {status} {test['name']}: {test['result']}")
        if test['result'] in ['FAIL', 'ERROR']:
            print(f"    Details: {test['details']}")
            
    return result['success']


async def analyze_native_transfers(chunk_file, target_address=None):
    """Analyze native ETH transfers in the chunk."""
    if not chunk_file:
        return
        
    print(f"\n💰 Analyzing native ETH transfers...")
    
    # Load and decompress chunk
    import gzip
    with gzip.open(chunk_file, 'rt', encoding='utf-8') as f:
        chunk_data = json.load(f)
        
    transfers = []
    total_eth_moved = 0
    
    # Analyze each block
    for block in chunk_data['blocks']:
        block_num = block['number']
        
        # Check each transaction
        for tx in block.get('transactions', []):
            value = int(tx.get('value', '0'))
            
            if value > 0:  # Native ETH transfer
                from_addr = tx.get('from', '').lower()
                to_addr = tx.get('to', '').lower()
                eth_value = value / 10**18  # Convert wei to ETH
                
                transfers.append({
                    'block': block_num,
                    'tx_hash': tx.get('hash'),
                    'from': from_addr,
                    'to': to_addr,
                    'value_eth': eth_value
                })
                
                total_eth_moved += eth_value
                
    print(f"📊 Transfer Analysis:")
    print(f"  Total native transfers: {len(transfers)}")
    print(f"  Total ETH moved: {total_eth_moved:.6f} ETH")
    
    if transfers:
        print(f"  Average transfer: {total_eth_moved/len(transfers):.6f} ETH")
        print(f"  Largest transfer: {max(t['value_eth'] for t in transfers):.6f} ETH")
        
    # If target address specified, calculate net transfers
    if target_address:
        target_address = target_address.lower()
        
        net_inbound = sum(t['value_eth'] for t in transfers if t['to'] == target_address)
        net_outbound = sum(t['value_eth'] for t in transfers if t['from'] == target_address)
        net_change = net_inbound - net_outbound
        
        print(f"\n🎯 Analysis for {target_address}:")
        print(f"  Inbound: {net_inbound:.6f} ETH")
        print(f"  Outbound: {net_outbound:.6f} ETH")
        print(f"  Net change: {net_change:+.6f} ETH")
        
    return transfers


async def main():
    """Run all live tests."""
    print("🛫 Starting live testing of Blocks app...")
    print("=" * 60)
    
    # Test trace verification
    trace_ok = await test_trace_verification()
    
    if not trace_ok:
        print("❌ Trace verification failed - using public RPC with limited capabilities")
    
    # Test block chunking
    chunk_file, chunk_data = await test_block_chunking()
    
    if chunk_file:
        # Test validation
        validation_ok = await test_validation(chunk_file, chunk_data)
        
        # Analyze transfers
        await analyze_native_transfers(chunk_file, 
                                     target_address="0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045")  # Vitalik's address
        
        print(f"\n🎯 Live Testing Results:")
        print(f"  Trace verification: {'✅' if trace_ok else '❌ (expected for public RPC)'}")
        print(f"  Block chunking: {'✅' if chunk_file else '❌'}")
        print(f"  Validation: {'✅' if validation_ok else '❌'}")
        print(f"  Transfer analysis: ✅")
        
    else:
        print("❌ Could not create test chunks")
        
    print("\n🏁 Live testing complete!")


if __name__ == "__main__":
    asyncio.run(main())