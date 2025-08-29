#!/usr/bin/env python3
"""
Get a REAL 10-minute chunk - 50 blocks of Ethereum data.
No more dummy 3-block samples!
"""

import json
import gzip
import hashlib
from pathlib import Path
from web3 import Web3
from datetime import datetime
import time

def get_full_10min_chunk():
    """Get actual 10-minute chunk with 50 blocks."""
    
    print("🚀 Creating FULL 10-minute Ethereum chunk (50 blocks)...")
    
    rpc_url = "https://ethereum.publicnode.com"
    w3 = Web3(Web3.HTTPProvider(rpc_url))
    
    if not w3.is_connected():
        print("❌ Failed to connect to Ethereum")
        return
        
    latest = w3.eth.block_number
    print(f"✅ Connected - Latest block: {latest}")
    
    # Get 50 consecutive blocks (real 10-minute chunk)
    start_block = latest - 60  # Go back a bit for finalized blocks
    end_block = start_block + 49  # 50 blocks total
    
    print(f"📦 Fetching blocks {start_block} to {end_block} (50 blocks)")
    print("⏳ This will take a few minutes with public RPC limits...")
    
    blocks_data = []
    total_txs = 0
    total_gas = 0
    
    start_time = time.time()
    
    for i, block_num in enumerate(range(start_block, end_block + 1)):
        print(f"  📥 [{i+1:2}/50] Fetching block {block_num}...", end="", flush=True)
        
        try:
            block = w3.eth.get_block(block_num, full_transactions=True)
            
            # Convert to serializable format
            block_data = {
                'number': block['number'],
                'hash': block['hash'].hex(),
                'parentHash': block['parentHash'].hex(),
                'timestamp': block['timestamp'],
                'miner': block['miner'],
                'difficulty': block['difficulty'],
                'totalDifficulty': int(block.get('totalDifficulty', 0)),
                'size': block['size'],
                'gasUsed': block['gasUsed'],
                'gasLimit': block['gasLimit'],
                'baseFeePerGas': int(block.get('baseFeePerGas', 0)),
                'transactions': []
            }
            
            # Process all transactions  
            for tx in block['transactions']:
                tx_data = {
                    'hash': tx['hash'].hex(),
                    'from': tx['from'],
                    'to': tx['to'],
                    'value': str(tx['value']),
                    'gas': tx['gas'],
                    'gasPrice': str(tx.get('gasPrice', 0)),
                    'maxFeePerGas': str(tx.get('maxFeePerGas', 0)) if tx.get('maxFeePerGas') else None,
                    'maxPriorityFeePerGas': str(tx.get('maxPriorityFeePerGas', 0)) if tx.get('maxPriorityFeePerGas') else None,
                    'nonce': tx['nonce'],
                    'input': tx['input'].hex() if hasattr(tx['input'], 'hex') else tx['input'],
                    'transactionIndex': tx['transactionIndex'],
                    'type': tx.get('type', 0)
                }
                block_data['transactions'].append(tx_data)
                
            blocks_data.append(block_data)
            total_txs += len(block_data['transactions'])
            total_gas += block_data['gasUsed']
            
            print(f" ✅ {len(block_data['transactions']):3} txs")
            
            # Rate limiting for public RPC
            time.sleep(0.1)
            
        except Exception as e:
            print(f" ❌ Error: {e}")
            continue
            
    elapsed_time = time.time() - start_time
    
    if not blocks_data:
        print("❌ No blocks fetched successfully")
        return
        
    print(f"\n✅ Fetched {len(blocks_data)} blocks in {elapsed_time:.1f} seconds")
    print(f"   Total transactions: {total_txs:,}")
    print(f"   Total gas used: {total_gas:,}")
    
    # Create full chunk structure
    chunk_data = {
        'version': '1.0',
        'chain': 'ethereum',
        'start_block': blocks_data[0]['number'],
        'end_block': blocks_data[-1]['number'],
        'created_at': datetime.utcnow().isoformat(),
        'blocks': blocks_data,
        'metadata': {
            'block_count': len(blocks_data),
            'total_transactions': total_txs,
            'total_gas_used': total_gas,
            'start_timestamp': blocks_data[0]['timestamp'],
            'end_timestamp': blocks_data[-1]['timestamp'],
            'time_span_seconds': blocks_data[-1]['timestamp'] - blocks_data[0]['timestamp']
        }
    }
    
    print(f"\n📊 Full Chunk Summary:")
    print(f"   Blocks: {len(blocks_data)}")
    print(f"   Time span: {chunk_data['metadata']['time_span_seconds']} seconds ({chunk_data['metadata']['time_span_seconds']/60:.1f} minutes)")
    print(f"   Transactions: {total_txs:,}")
    print(f"   Gas used: {total_gas:,}")
    
    # Compress and measure
    print(f"\n🗜️  Compressing chunk...")
    json_str = json.dumps(chunk_data, separators=(',', ':'))
    json_bytes = json_str.encode('utf-8')
    compressed = gzip.compress(json_bytes, compresslevel=9)
    
    # Size analysis
    uncompressed_size = len(json_bytes)
    compressed_size = len(compressed)
    compression_ratio = uncompressed_size / compressed_size
    
    print(f"📏 Size Analysis:")
    print(f"   Uncompressed: {uncompressed_size:,} bytes ({uncompressed_size/1024/1024:.2f} MB)")
    print(f"   Compressed: {compressed_size:,} bytes ({compressed_size/1024:.2f} KB)")
    print(f"   Compression ratio: {compression_ratio:.2f}x")
    
    # Save the full chunk
    output_dir = Path('./real_chunks')
    output_dir.mkdir(exist_ok=True)
    
    timestamp = blocks_data[0]['timestamp']
    dt = datetime.fromtimestamp(timestamp)
    filename = f"eth_10min_{chunk_data['start_block']}_{chunk_data['end_block']}_{timestamp}.json.gz"
    filepath = output_dir / filename
    
    print(f"\n💾 Saving full 10-minute chunk...")
    with open(filepath, 'wb') as f:
        f.write(compressed)
        
    file_size = filepath.stat().st_size
    file_hash = hashlib.sha256(compressed).hexdigest()
    
    print(f"✅ Saved: {filepath}")
    print(f"   File size: {file_size:,} bytes ({file_size/1024:.2f} KB)")
    print(f"   SHA256: {file_hash[:32]}...")
    
    # Verify decompression
    print(f"\n🧪 Verifying integrity...")
    with gzip.open(filepath, 'rt', encoding='utf-8') as f:
        loaded = json.load(f)
        
    if loaded == chunk_data:
        print("✅ Integrity verified - data perfect")
    else:
        print("❌ Integrity check failed")
        
    # Analyze native ETH transfers
    print(f"\n💰 Analyzing native ETH transfers...")
    transfers = []
    total_eth_moved = 0
    addresses = set()
    
    for block in blocks_data:
        for tx in block['transactions']:
            value_wei = int(tx['value'])
            if value_wei > 0:
                eth_value = value_wei / 10**18
                from_addr = tx['from'].lower()
                to_addr = tx['to'].lower() if tx['to'] else None
                
                transfers.append({
                    'from': from_addr,
                    'to': to_addr,
                    'value_eth': eth_value,
                    'tx_hash': tx['hash'],
                    'block': block['number']
                })
                
                total_eth_moved += eth_value
                addresses.add(from_addr)
                if to_addr:
                    addresses.add(to_addr)
                    
    print(f"📈 Transfer Analysis:")
    print(f"   Native ETH transfers: {len(transfers):,}")
    print(f"   Total ETH moved: {total_eth_moved:,.6f} ETH")
    print(f"   Average transfer: {total_eth_moved/len(transfers):.6f} ETH" if transfers else "   No transfers")
    print(f"   Largest transfer: {max(t['value_eth'] for t in transfers):.6f} ETH" if transfers else "   No transfers")
    print(f"   Unique addresses: {len(addresses):,}")
    
    # Test specific address net calculations
    test_addresses = [
        "0xd8da6bf26964af9d7eed9e03e53415d37aa96045",  # Vitalik
        "0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45",  # Uniswap V3
        "0xa0b86a33e6180fb35d0b65bfd1e1e2c6b61e66f1",  # Popular address
        "0x28c6c06298d514db089934071355e5743bf21d60"   # Binance Hot Wallet 14
    ]
    
    print(f"\n🎯 Net transfer calculations for popular addresses:")
    
    for addr in test_addresses:
        addr_lower = addr.lower()
        inbound = sum(t['value_eth'] for t in transfers if t['to'] == addr_lower)
        outbound = sum(t['value_eth'] for t in transfers if t['from'] == addr_lower)
        net = inbound - outbound
        
        if inbound > 0 or outbound > 0:
            print(f"   {addr[:10]}...{addr[-8:]}:")
            print(f"     In: {inbound:.6f} ETH, Out: {outbound:.6f} ETH, Net: {net:+.6f} ETH")
        else:
            print(f"   {addr[:10]}...{addr[-8:]}: No activity")
    
    print(f"\n🎉 FULL 10-MINUTE CHUNK COMPLETE!")
    print(f"   📁 {file_size:,} bytes of compressed Ethereum blockchain data")
    print(f"   ⏱️  {chunk_data['metadata']['time_span_seconds']/60:.1f} minutes of blockchain history")
    print(f"   💸 {total_eth_moved:,.2f} ETH in native transfers")
    
    return filepath, chunk_data, transfers


if __name__ == "__main__":
    get_full_10min_chunk()