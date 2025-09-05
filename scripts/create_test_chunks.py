#!/usr/bin/env python
"""
Create test chunks for pipeline testing
These are minimal test chunks, not real blockchain data
"""
import json
import gzip
from datetime import date, timedelta
from pathlib import Path
import sys
import os
sys.path.insert(0, '/home/dev/p/boundcorp/zeroindex')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'zeroindex.settings')
import django
django.setup()

from zeroindex.apps.blocks.models import Chunk
from zeroindex.apps.chains.models import Chain

def create_test_chunk(chunk_date, start_block, end_block):
    """Create a minimal test chunk for a given date"""
    
    # Get Ethereum chain
    chain = Chain.objects.get(chain_id=1)
    
    # Create minimal test data
    blocks = []
    for block_num in range(start_block, min(start_block + 10, end_block + 1)):  # Only 10 blocks for test
        blocks.append({
            'number': block_num,
            'hash': f'0x{"0" * 62}{block_num:02x}',
            'parent_hash': f'0x{"0" * 62}{block_num-1:02x}',
            'timestamp': 1661700000 + (block_num * 12),  # ~12 seconds per block
            'miner': '0x' + '0' * 40,
            'gas_limit': 30000000,
            'gas_used': 15000000,
            'base_fee_per_gas': 1000000000,
            'transaction_count': 5,
            'transactions': []
        })
    
    chunk_data = {
        'chunk_date': chunk_date.isoformat(),
        'start_block': start_block,
        'end_block': end_block,
        'blocks': blocks,
        'metadata': {
            'total_blocks': len(blocks),
            'is_test_data': True,
            'created_at': date.today().isoformat()
        }
    }
    
    # Save to file
    file_path = Path(f'data/test_chunks/test_chunk_{chunk_date}.json.gz')
    file_path.parent.mkdir(parents=True, exist_ok=True)
    
    with gzip.open(file_path, 'wt') as f:
        json.dump(chunk_data, f, indent=2)
    
    # Create or update database record
    try:
        chunk = Chunk.objects.filter(chunk_date=chunk_date, chain=chain).first()
        if chunk:
            print(f'⏭️  Test chunk already exists for {chunk_date}')
            created = False
        else:
            chunk = Chunk.objects.create(
                chain=chain,
                chunk_date=chunk_date,
                start_block=start_block,
                end_block=end_block,
                status='complete',
                completeness_percentage=100.0,
                file_path=str(file_path),
                file_size_bytes=file_path.stat().st_size,
                total_blocks=len(blocks),
                total_transactions=0,
            )
            created = True
    except Exception as e:
        print(f'⚠️  Error for {chunk_date}: {str(e)}')
        return None
    
    if created:
        print(f'✅ Created test chunk for {chunk_date}')
    else:
        print(f'⏭️  Test chunk already exists for {chunk_date}')
    
    return chunk

def main():
    # Create test chunks for dates we're missing
    base_block = 23200000  # Arbitrary starting point
    blocks_per_day = 7142  # Approximate
    
    # Create chunks for full month of August 2025 (except 27th which we have real data)
    dates_to_create = []
    for day in range(1, 32):  # August has 31 days
        chunk_date = date(2025, 8, day)
        if chunk_date != date(2025, 8, 27):  # Skip 27th, we have real data
            dates_to_create.append(chunk_date)
    
    for i, chunk_date in enumerate(dates_to_create):
        start_block = base_block + (i * blocks_per_day)
        end_block = start_block + blocks_per_day - 1
        create_test_chunk(chunk_date, start_block, end_block)
    
    print('\n📊 Test chunks created successfully!')
    print('Note: These are minimal test chunks, not real blockchain data')

if __name__ == '__main__':
    main()