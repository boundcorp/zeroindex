#!/usr/bin/env python3
"""
Create a real 10-minute chunk and analyze it.
This will show actual blockchain data processing with size measurements.
"""

import asyncio
import sys
import os
import gzip
import json
from pathlib import Path
from datetime import datetime
from web3 import Web3

# Add current dir to path
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

from block_chunker import BlockChunker


async def create_and_analyze_chunk():
    """Create a real 10-minute chunk and analyze its contents."""
    
    print("🔗 Connecting to Ethereum...")
    
    # Use public RPC
    rpc_url = "https://ethereum.publicnode.com"
    chunker = BlockChunker(rpc_url, './real_data')
    
    if not chunker.w3.is_connected():
        print("❌ Failed to connect")
        return
        
    latest_block = chunker.w3.eth.block_number
    print(f"✅ Connected - Latest block: {latest_block}")
    
    # Get 50 consecutive blocks (approximately 10 minutes)
    start_block = latest_block - 60  # Go back a bit to ensure blocks are final
    end_block = start_block + 49     # 50 blocks total
    
    print(f"\n📦 Creating chunk for blocks {start_block} to {end_block}")
    print("This represents approximately 10 minutes of Ethereum blockchain data...")
    
    try:
        # Create the chunk
        print("⏳ Fetching block data...")
        chunk_data = chunker.create_chunk(start_block, end_block)
        
        print(f"✅ Successfully created chunk with {len(chunk_data['blocks'])} blocks")
        
        # Save and get measurements
        filename = chunker.save_chunk(chunk_data)
        chunk_file = chunker.chunks_dir / filename
        
        # File size measurements
        compressed_size = chunk_file.stat().st_size
        
        # Calculate uncompressed size
        json_str = json.dumps(chunk_data, separators=(',', ':'))
        uncompressed_size = len(json_str.encode('utf-8'))
        
        compression_ratio = uncompressed_size / compressed_size
        
        print(f"\n📊 Chunk Analysis:")
        print(f"📁 Filename: {filename}")
        print(f"🗜️  Compressed size: {compressed_size:,} bytes ({compressed_size/1024:.2f} KB)")
        print(f"📄 Uncompressed size: {uncompressed_size:,} bytes ({uncompressed_size/1024:.2f} KB)")  
        print(f"🔄 Compression ratio: {compression_ratio:.2f}x")
        
        # Block metadata
        metadata = chunk_data['metadata']
        print(f"\n⛓️  Block Details:")
        print(f"📊 Blocks: {metadata['block_count']}")
        print(f"🔄 Total transactions: {metadata['total_transactions']}")
        print(f"⛽ Total gas used: {metadata['total_gas_used']:,}")
        print(f"⏰ Time span: {metadata['end_timestamp'] - metadata['start_timestamp']} seconds")
        
        # Analyze transactions and transfers
        print(f"\n💰 Analyzing native ETH transfers...")
        total_eth_moved = 0
        native_transfers = []
        addresses_involved = set()
        
        for block in chunk_data['blocks']:
            for tx in block['transactions']:
                value_wei = int(tx['value'])
                if value_wei > 0:
                    eth_value = value_wei / 10**18
                    native_transfers.append({
                        'from': tx['from'].lower(),
                        'to': tx['to'].lower() if tx['to'] else None,
                        'value_eth': eth_value,
                        'tx_hash': tx['hash'],
                        'block': block['number']
                    })
                    total_eth_moved += eth_value
                    addresses_involved.add(tx['from'].lower())
                    if tx['to']:
                        addresses_involved.add(tx['to'].lower())
                        
        print(f"💸 Native ETH transfers: {len(native_transfers)}")
        print(f"💰 Total ETH moved: {total_eth_moved:.6f} ETH")
        if native_transfers:
            print(f"📊 Average transfer: {total_eth_moved/len(native_transfers):.6f} ETH")
            print(f"🏆 Largest transfer: {max(t['value_eth'] for t in native_transfers):.6f} ETH")
        print(f"👥 Unique addresses involved: {len(addresses_involved)}")
        
        # Test specific address analysis
        target_address = "0xd8da6bf26964af9d7eed9e03e53415d37aa96045"  # Vitalik's address
        print(f"\n🎯 Analysis for target address: {target_address}")
        
        inbound = sum(t['value_eth'] for t in native_transfers 
                     if t['to'] == target_address.lower())
        outbound = sum(t['value_eth'] for t in native_transfers 
                      if t['from'] == target_address.lower())
        net_change = inbound - outbound
        
        print(f"📈 Inbound: {inbound:.6f} ETH")
        print(f"📉 Outbound: {outbound:.6f} ETH")  
        print(f"🔄 Net change: {net_change:+.6f} ETH")
        
        # Try another popular address
        uniswap_v3 = "0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45"  # Uniswap V3 Router
        print(f"\n🦄 Analysis for Uniswap V3 Router: {uniswap_v3}")
        
        inbound_uni = sum(t['value_eth'] for t in native_transfers 
                         if t['to'] == uniswap_v3.lower())
        outbound_uni = sum(t['value_eth'] for t in native_transfers 
                          if t['from'] == uniswap_v3.lower())
        net_change_uni = inbound_uni - outbound_uni
        
        print(f"📈 Inbound: {inbound_uni:.6f} ETH")
        print(f"📉 Outbound: {outbound_uni:.6f} ETH")
        print(f"🔄 Net change: {net_change_uni:+.6f} ETH")
        
        print(f"\n✅ Real 10-minute chunk analysis complete!")
        print(f"💾 Data saved to: {chunk_file}")
        
        # Verify we can decompress and read it back
        print(f"\n🧪 Verifying chunk integrity...")
        with gzip.open(chunk_file, 'rt', encoding='utf-8') as f:
            loaded_data = json.load(f)
            
        if loaded_data == chunk_data:
            print("✅ Chunk integrity verified - data matches perfectly")
        else:
            print("❌ Chunk integrity failed")
            
        return chunk_file, chunk_data, native_transfers
        
    except Exception as e:
        print(f"❌ Error creating chunk: {e}")
        import traceback
        traceback.print_exc()
        return None, None, None


if __name__ == "__main__":
    asyncio.run(create_and_analyze_chunk())