#!/usr/bin/env python3
"""
Quick test - create a small chunk with real blockchain data.
"""

import json
import gzip
import hashlib
from pathlib import Path
from web3 import Web3
from datetime import datetime

def create_test_chunk():
    """Create a small test chunk with real data."""
    
    print("🔗 Connecting to Ethereum...")
    rpc_url = "https://ethereum.publicnode.com"
    w3 = Web3(Web3.HTTPProvider(rpc_url))
    
    if not w3.is_connected():
        print("❌ Failed to connect")
        return
        
    latest = w3.eth.block_number
    print(f"✅ Connected - Latest block: {latest}")
    
    # Get just 3 recent blocks for testing
    test_blocks = []
    start_block = latest - 5
    
    for i in range(3):
        block_num = start_block + i
        print(f"📦 Fetching block {block_num}...")
        
        try:
            block = w3.eth.get_block(block_num, full_transactions=True)
            
            # Convert to serializable format
            block_data = {
                'number': block['number'],
                'hash': block['hash'].hex(),
                'timestamp': block['timestamp'],
                'gasUsed': block['gasUsed'],
                'transactions': []
            }
            
            # Process transactions
            for tx in block['transactions']:
                tx_data = {
                    'hash': tx['hash'].hex(),
                    'from': tx['from'],
                    'to': tx['to'],
                    'value': str(tx['value']),
                    'gas': tx['gas'],
                    'gasPrice': str(tx.get('gasPrice', 0))
                }
                block_data['transactions'].append(tx_data)
                
            test_blocks.append(block_data)
            print(f"  ✅ Block {block_num}: {len(block_data['transactions'])} transactions")
            
        except Exception as e:
            print(f"  ❌ Error fetching block {block_num}: {e}")
            
    if not test_blocks:
        print("❌ No blocks fetched")
        return
        
    # Create chunk structure
    chunk_data = {
        'version': '1.0',
        'chain': 'ethereum',
        'start_block': test_blocks[0]['number'],
        'end_block': test_blocks[-1]['number'],
        'created_at': datetime.utcnow().isoformat(),
        'blocks': test_blocks
    }
    
    # Calculate metadata
    total_txs = sum(len(b['transactions']) for b in test_blocks)
    chunk_data['metadata'] = {
        'block_count': len(test_blocks),
        'total_transactions': total_txs,
        'start_timestamp': test_blocks[0]['timestamp'],
        'end_timestamp': test_blocks[-1]['timestamp']
    }
    
    print(f"\n📊 Chunk Summary:")
    print(f"  Blocks: {len(test_blocks)}")
    print(f"  Total transactions: {total_txs}")
    
    # Compress and save
    json_str = json.dumps(chunk_data, separators=(',', ':'))
    json_bytes = json_str.encode('utf-8')
    compressed = gzip.compress(json_bytes, compresslevel=9)
    
    # File sizes
    uncompressed_size = len(json_bytes)
    compressed_size = len(compressed)
    compression_ratio = uncompressed_size / compressed_size
    
    print(f"\n📏 Size Analysis:")
    print(f"  Uncompressed: {uncompressed_size:,} bytes ({uncompressed_size/1024:.2f} KB)")
    print(f"  Compressed: {compressed_size:,} bytes ({compressed_size/1024:.2f} KB)")
    print(f"  Compression ratio: {compression_ratio:.2f}x")
    
    # Save to file
    output_dir = Path('./test_data/chunks')
    output_dir.mkdir(parents=True, exist_ok=True)
    
    filename = f"eth_test_{chunk_data['start_block']}_{chunk_data['end_block']}.json.gz"
    filepath = output_dir / filename
    
    with open(filepath, 'wb') as f:
        f.write(compressed)
        
    # Calculate file hash
    file_hash = hashlib.sha256(compressed).hexdigest()
    
    print(f"\n💾 Saved to: {filepath}")
    print(f"   Size on disk: {filepath.stat().st_size:,} bytes")
    print(f"   SHA256: {file_hash[:16]}...")
    
    # Verify we can read it back
    print(f"\n🧪 Verifying decompression...")
    with gzip.open(filepath, 'rt', encoding='utf-8') as f:
        loaded_data = json.load(f)
        
    if loaded_data == chunk_data:
        print("✅ Decompression successful - data matches")
    else:
        print("❌ Decompression failed")
        
    # Analyze native ETH transfers
    print(f"\n💰 Analyzing native ETH transfers...")
    transfers = []
    total_eth = 0
    
    for block in test_blocks:
        for tx in block['transactions']:
            value_wei = int(tx['value'])
            if value_wei > 0:
                eth_value = value_wei / 10**18
                transfers.append({
                    'from': tx['from'].lower(),
                    'to': tx['to'].lower() if tx['to'] else None,
                    'value_eth': eth_value,
                    'tx_hash': tx['hash'],
                    'block': block['number']
                })
                total_eth += eth_value
                
    print(f"  Native transfers found: {len(transfers)}")
    print(f"  Total ETH moved: {total_eth:.6f} ETH")
    
    if transfers:
        print(f"  Average transfer: {total_eth/len(transfers):.6f} ETH")
        print(f"  Largest transfer: {max(t['value_eth'] for t in transfers):.6f} ETH")
        
    # Test specific address calculation
    target_address = "0xd8da6bf26964af9d7eed9e03e53415d37aa96045"  # Vitalik
    print(f"\n🎯 Net transfers for {target_address}:")
    
    inbound = sum(t['value_eth'] for t in transfers if t['to'] == target_address.lower())
    outbound = sum(t['value_eth'] for t in transfers if t['from'] == target_address.lower())
    net_change = inbound - outbound
    
    print(f"  Inbound: {inbound:.6f} ETH")
    print(f"  Outbound: {outbound:.6f} ETH")
    print(f"  Net change: {net_change:+.6f} ETH")
    
    print(f"\n✅ Test complete! Created and analyzed real blockchain chunk.")
    
    return filepath, chunk_data, transfers


if __name__ == "__main__":
    create_test_chunk()