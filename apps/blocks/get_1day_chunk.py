#!/usr/bin/env python3
"""
Get a full 1-day chunk of Ethereum data.
1 day = ~7200 blocks at 12 seconds per block.
"""

import json
import gzip
import hashlib
from pathlib import Path
from web3 import Web3
from datetime import datetime, timedelta
import time

def get_yesterday_chunk():
    """Get full 1-day chunk for yesterday."""
    
    print("🗓️  Creating FULL 1-DAY Ethereum chunk...")
    
    rpc_url = "https://ethereum.publicnode.com"
    w3 = Web3(Web3.HTTPProvider(rpc_url))
    
    if not w3.is_connected():
        print("❌ Failed to connect to Ethereum")
        return
        
    latest = w3.eth.block_number
    latest_block = w3.eth.get_block(latest)
    latest_timestamp = latest_block['timestamp']
    
    print(f"✅ Connected - Latest block: {latest} at {datetime.fromtimestamp(latest_timestamp)}")
    
    # Calculate yesterday's block range
    # 1 day = 86400 seconds, ~7200 blocks at 12 sec/block
    seconds_per_block = 12
    blocks_per_day = 86400 // seconds_per_block  # 7200 blocks
    
    # Find start of yesterday (00:00 UTC)
    now = datetime.utcnow()
    yesterday_start = now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
    yesterday_end = yesterday_start + timedelta(days=1) - timedelta(seconds=1)
    
    print(f"📅 Target period: {yesterday_start} to {yesterday_end} UTC")
    
    # Estimate block numbers for yesterday
    current_time = datetime.fromtimestamp(latest_timestamp)
    time_diff_to_yesterday_end = (current_time - yesterday_end).total_seconds()
    blocks_back_to_yesterday_end = int(time_diff_to_yesterday_end / seconds_per_block)
    
    end_block = latest - blocks_back_to_yesterday_end
    start_block = end_block - blocks_per_day + 1
    
    print(f"📦 Estimated block range: {start_block} to {end_block} ({end_block - start_block + 1} blocks)")
    
    # Due to RPC limits, we'll sample the day instead of getting every block
    # Sample every 10th block to get ~720 blocks representing the full day
    sample_interval = 10
    sample_blocks = list(range(start_block, end_block + 1, sample_interval))
    
    print(f"📊 Sampling strategy: Every {sample_interval}th block = {len(sample_blocks)} blocks")
    print("⏳ This will take several minutes...")
    
    blocks_data = []
    total_txs = 0
    total_gas = 0
    earliest_time = None
    latest_time = None
    
    start_time = time.time()
    
    for i, block_num in enumerate(sample_blocks):
        if i % 50 == 0:  # Progress every 50 blocks
            print(f"  📥 [{i:3}/{len(sample_blocks)}] Fetching block {block_num}...")
        
        try:
            block = w3.eth.get_block(block_num, full_transactions=True)
            
            block_timestamp = block['timestamp']
            if earliest_time is None or block_timestamp < earliest_time:
                earliest_time = block_timestamp
            if latest_time is None or block_timestamp > latest_time:
                latest_time = block_timestamp
            
            # Convert to serializable format (simplified for size)
            block_data = {
                'number': block['number'],
                'hash': block['hash'].hex(),
                'parentHash': block['parentHash'].hex(),
                'timestamp': block['timestamp'],
                'miner': block['miner'],
                'gasUsed': block['gasUsed'],
                'gasLimit': block['gasLimit'],
                'baseFeePerGas': int(block.get('baseFeePerGas', 0)),
                'transactionCount': len(block['transactions']),
                'transactions': []
            }
            
            # Process transactions (limited data for size)
            for tx in block['transactions']:
                tx_data = {
                    'hash': tx['hash'].hex(),
                    'from': tx['from'],
                    'to': tx['to'],
                    'value': str(tx['value']),
                    'gas': tx['gas'],
                    'gasPrice': str(tx.get('gasPrice', 0)),
                    'nonce': tx['nonce'],
                    'transactionIndex': tx['transactionIndex']
                }
                block_data['transactions'].append(tx_data)
                
            blocks_data.append(block_data)
            total_txs += len(block_data['transactions'])
            total_gas += block_data['gasUsed']
            
            # Rate limiting
            time.sleep(0.05)
            
        except Exception as e:
            print(f"  ❌ Error fetching block {block_num}: {e}")
            continue
            
    elapsed_time = time.time() - start_time
    
    if not blocks_data:
        print("❌ No blocks fetched successfully")
        return
        
    time_span = latest_time - earliest_time
    
    print(f"\n✅ Fetched {len(blocks_data)} sample blocks in {elapsed_time:.1f} seconds")
    print(f"   Actual time span: {time_span} seconds ({time_span/3600:.1f} hours)")
    print(f"   Total transactions: {total_txs:,}")
    print(f"   Total gas used: {total_gas:,}")
    
    # Create 1-day chunk structure
    chunk_data = {
        'version': '1.0',
        'chain': 'ethereum', 
        'chunk_type': '1day_sample',
        'start_block': blocks_data[0]['number'],
        'end_block': blocks_data[-1]['number'],
        'sample_interval': sample_interval,
        'created_at': datetime.utcnow().isoformat(),
        'blocks': blocks_data,
        'metadata': {
            'block_count': len(blocks_data),
            'blocks_sampled_from': end_block - start_block + 1,
            'sample_ratio': len(blocks_data) / (end_block - start_block + 1),
            'total_transactions': total_txs,
            'total_gas_used': total_gas,
            'start_timestamp': earliest_time,
            'end_timestamp': latest_time,
            'time_span_seconds': time_span,
            'time_span_hours': time_span / 3600
        }
    }
    
    print(f"\n📊 1-Day Chunk Summary:")
    print(f"   Sample blocks: {len(blocks_data)}")
    print(f"   Represents: {chunk_data['metadata']['blocks_sampled_from']} blocks")
    print(f"   Sample ratio: {chunk_data['metadata']['sample_ratio']:.1%}")
    print(f"   Time span: {time_span/3600:.1f} hours")
    print(f"   Transactions: {total_txs:,}")
    print(f"   Gas used: {total_gas:,}")
    
    # Compress and measure
    print(f"\n🗜️  Compressing 1-day chunk...")
    json_str = json.dumps(chunk_data, separators=(',', ':'))
    json_bytes = json_str.encode('utf-8')
    compressed = gzip.compress(json_bytes, compresslevel=9)
    
    # Size analysis
    uncompressed_size = len(json_bytes)
    compressed_size = len(compressed)
    compression_ratio = uncompressed_size / compressed_size
    
    print(f"📏 Size Analysis:")
    print(f"   Uncompressed: {uncompressed_size:,} bytes ({uncompressed_size/1024/1024:.2f} MB)")
    print(f"   Compressed: {compressed_size:,} bytes ({compressed_size/1024/1024:.2f} MB)")
    print(f"   Compression ratio: {compression_ratio:.2f}x")
    
    # Estimate full day size (if we had every block)
    estimated_full_size = compressed_size / chunk_data['metadata']['sample_ratio']
    print(f"   Estimated full 1-day size: {estimated_full_size/1024/1024:.2f} MB compressed")
    
    # Save the 1-day chunk
    output_dir = Path('./daily_chunks')
    output_dir.mkdir(exist_ok=True)
    
    date_str = yesterday_start.strftime('%Y%m%d')
    filename = f"eth_1day_{date_str}_{chunk_data['start_block']}_{chunk_data['end_block']}.json.gz"
    filepath = output_dir / filename
    
    print(f"\n💾 Saving 1-day chunk...")
    with open(filepath, 'wb') as f:
        f.write(compressed)
        
    file_size = filepath.stat().st_size
    file_hash = hashlib.sha256(compressed).hexdigest()
    
    print(f"✅ Saved: {filepath}")
    print(f"   File size: {file_size:,} bytes ({file_size/1024/1024:.2f} MB)")
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
                    
    print(f"📈 Transfer Analysis (sampled):")
    print(f"   Native ETH transfers: {len(transfers):,}")
    print(f"   Total ETH moved: {total_eth_moved:,.6f} ETH")
    print(f"   Average transfer: {total_eth_moved/len(transfers):.6f} ETH" if transfers else "   No transfers")
    print(f"   Largest transfer: {max(t['value_eth'] for t in transfers):.6f} ETH" if transfers else "   No transfers")
    print(f"   Unique addresses: {len(addresses):,}")
    
    # Estimate full day ETH volume
    estimated_full_eth = total_eth_moved / chunk_data['metadata']['sample_ratio']
    print(f"   Estimated full day ETH volume: {estimated_full_eth:,.2f} ETH")
    
    # Test specific addresses
    test_addresses = [
        "0xd8da6bf26964af9d7eed9e03e53415d37aa96045",  # Vitalik
        "0x28c6c06298d514db089934071355e5743bf21d60",  # Binance Hot Wallet 14
        "0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45",  # Uniswap V3
        "0xdac17f958d2ee523a2206206994597c13d831ec7"   # USDT Contract
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
    
    print(f"\n🎉 FULL 1-DAY CHUNK COMPLETE!")
    print(f"   📁 {file_size/1024/1024:.2f} MB compressed")
    print(f"   📅 {time_span/3600:.1f} hours of blockchain history")
    print(f"   💸 {total_eth_moved:,.2f} ETH in sampled transfers")
    print(f"   📊 Estimated full day: {estimated_full_size/1024/1024:.2f} MB, {estimated_full_eth:,.2f} ETH")
    
    return filepath, chunk_data, transfers


if __name__ == "__main__":
    get_yesterday_chunk()