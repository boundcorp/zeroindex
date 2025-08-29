#!/usr/bin/env python3
"""
Block chunking and compression module.
Designed to package blockchain data efficiently - coding from the clouds.
"""

import gzip
import json
import os
import time
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
from web3 import Web3, HTTPProvider
from web3.types import BlockData
import hashlib
import asyncio
from concurrent.futures import ThreadPoolExecutor


class BlockChunker:
    """
    Chunks blockchain data into 10-minute intervals and compresses them.
    Optimized for S3 storage and efficient retrieval.
    """
    
    BLOCKS_PER_CHUNK = 50  # ~10 minutes at 12 sec/block
    CHUNK_DURATION_SECONDS = 600  # 10 minutes
    
    def __init__(self, rpc_url: Optional[str] = None, data_dir: Optional[str] = None):
        """Initialize chunker with RPC and storage paths."""
        self.rpc_url = rpc_url or os.getenv('ETH_RPC_URL', 'http://ethereum-l1-node:8545')
        self.w3 = Web3(HTTPProvider(self.rpc_url))
        self.data_dir = Path(data_dir or os.getenv('BLOCKS_DATA_DIR', './data/blocks'))
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # Create subdirectories
        self.raw_dir = self.data_dir / 'raw'
        self.chunks_dir = self.data_dir / 'chunks'
        self.raw_dir.mkdir(exist_ok=True)
        self.chunks_dir.mkdir(exist_ok=True)
        
    def get_block_with_traces(self, block_num: int) -> Dict[str, Any]:
        """
        Fetch block with all associated trace data.
        Combines multiple data sources for complete picture.
        """
        try:
            # Get basic block data
            block = self.w3.eth.get_block(block_num, full_transactions=True)
            
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
            
            # Get receipts for all transactions
            receipts = []
            if block['transactions']:
                receipt_batch = self.w3.provider.make_request(
                    'eth_getBlockReceipts',
                    [hex(block_num)]
                )
                receipts = receipt_batch.get('result', [])
            
            # Process each transaction with traces
            for i, tx in enumerate(block['transactions']):
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
                    'receipt': receipts[i] if i < len(receipts) else None
                }
                
                # Try to get traces
                try:
                    trace_result = self.w3.provider.make_request(
                        'debug_traceTransaction',
                        [tx['hash'].hex(), {'tracer': 'callTracer'}]
                    )
                    tx_data['trace'] = trace_result.get('result', {})
                except:
                    # Fallback to basic trace if debug not available
                    try:
                        trace_result = self.w3.provider.make_request(
                            'trace_transaction',
                            [tx['hash'].hex()]
                        )
                        tx_data['trace'] = trace_result.get('result', [])
                    except:
                        tx_data['trace'] = None
                        
                block_data['transactions'].append(tx_data)
                
            # Get uncle/ommer blocks if any
            if block.get('uncles'):
                block_data['uncles'] = [u.hex() if hasattr(u, 'hex') else u for u in block['uncles']]
                
            return block_data
            
        except Exception as e:
            print(f"Error fetching block {block_num}: {e}")
            raise
            
    def create_chunk(self, start_block: int, end_block: int) -> Dict[str, Any]:
        """
        Create a chunk containing blocks from start to end.
        Returns metadata about the chunk.
        """
        print(f"✈️ Creating chunk for blocks {start_block}-{end_block} (flying high)")
        
        chunk_data = {
            'version': '1.0',
            'chain': 'ethereum',
            'start_block': start_block,
            'end_block': end_block,
            'created_at': datetime.utcnow().isoformat(),
            'blocks': []
        }
        
        # Fetch all blocks in range
        for block_num in range(start_block, end_block + 1):
            try:
                block_data = self.get_block_with_traces(block_num)
                chunk_data['blocks'].append(block_data)
            except Exception as e:
                print(f"Failed to fetch block {block_num}: {e}")
                # Continue with partial chunk
                
        if not chunk_data['blocks']:
            raise ValueError("No blocks fetched for chunk")
            
        # Calculate chunk metadata
        chunk_data['metadata'] = {
            'block_count': len(chunk_data['blocks']),
            'start_timestamp': chunk_data['blocks'][0]['timestamp'],
            'end_timestamp': chunk_data['blocks'][-1]['timestamp'],
            'total_transactions': sum(len(b['transactions']) for b in chunk_data['blocks']),
            'total_gas_used': sum(b['gasUsed'] for b in chunk_data['blocks'])
        }
        
        return chunk_data
        
    def compress_chunk(self, chunk_data: Dict[str, Any]) -> Tuple[bytes, Dict[str, Any]]:
        """
        Compress chunk data and return compressed bytes with metadata.
        """
        # Convert to JSON
        json_data = json.dumps(chunk_data, separators=(',', ':'))
        json_bytes = json_data.encode('utf-8')
        
        # Compress with maximum compression
        compressed = gzip.compress(json_bytes, compresslevel=9)
        
        # Calculate checksums
        metadata = {
            'original_size': len(json_bytes),
            'compressed_size': len(compressed),
            'compression_ratio': len(json_bytes) / len(compressed),
            'sha256': hashlib.sha256(compressed).hexdigest(),
            'blocks': f"{chunk_data['start_block']}-{chunk_data['end_block']}"
        }
        
        return compressed, metadata
        
    def save_chunk(self, chunk_data: Dict[str, Any]) -> str:
        """
        Save compressed chunk to disk and return filename.
        """
        # Generate filename based on block range and timestamp
        start = chunk_data['start_block']
        end = chunk_data['end_block']
        timestamp = chunk_data['blocks'][0]['timestamp']
        
        filename = f"eth_blocks_{start:08d}_{end:08d}_{timestamp}.json.gz"
        filepath = self.chunks_dir / filename
        
        # Compress and save
        compressed, metadata = self.compress_chunk(chunk_data)
        
        with open(filepath, 'wb') as f:
            f.write(compressed)
            
        # Save metadata
        meta_file = self.chunks_dir / f"{filename}.meta"
        with open(meta_file, 'w') as f:
            json.dump(metadata, f, indent=2)
            
        print(f"💾 Saved chunk {filename} - {metadata['compressed_size']:,} bytes "
              f"(ratio: {metadata['compression_ratio']:.2f}x)")
        
        return filename
        
    def process_block_range(self, start_block: int, end_block: int, 
                           chunk_size: Optional[int] = None) -> List[str]:
        """
        Process a range of blocks into chunks.
        Returns list of saved chunk filenames.
        """
        chunk_size = chunk_size or self.BLOCKS_PER_CHUNK
        saved_chunks = []
        
        print(f"🛫 Processing blocks {start_block} to {end_block} at 30,000 feet")
        
        current = start_block
        while current <= end_block:
            chunk_end = min(current + chunk_size - 1, end_block)
            
            try:
                # Create and save chunk
                chunk_data = self.create_chunk(current, chunk_end)
                filename = self.save_chunk(chunk_data)
                saved_chunks.append(filename)
                
            except Exception as e:
                print(f"Error processing chunk {current}-{chunk_end}: {e}")
                
            current = chunk_end + 1
            
        return saved_chunks
        
    def find_time_based_chunks(self, target_time: datetime) -> Optional[str]:
        """
        Find chunk file containing blocks from a specific time.
        """
        # Estimate block number from timestamp
        latest_block = self.w3.eth.get_block('latest')
        latest_time = datetime.fromtimestamp(latest_block['timestamp'])
        
        time_diff = (latest_time - target_time).total_seconds()
        blocks_diff = int(time_diff / 12)  # ~12 seconds per block
        
        target_block = latest_block['number'] - blocks_diff
        
        # Look for chunk file
        for chunk_file in self.chunks_dir.glob("*.json.gz"):
            if chunk_file.name.endswith('.meta'):
                continue
                
            # Parse filename
            parts = chunk_file.stem.split('_')
            if len(parts) >= 4:
                start = int(parts[2])
                end = int(parts[3])
                
                if start <= target_block <= end:
                    return chunk_file.name
                    
        return None
        
    def verify_chunk(self, filename: str) -> bool:
        """
        Verify integrity of a saved chunk.
        """
        filepath = self.chunks_dir / filename
        meta_file = self.chunks_dir / f"{filename}.meta"
        
        if not filepath.exists() or not meta_file.exists():
            return False
            
        # Load metadata
        with open(meta_file, 'r') as f:
            metadata = json.load(f)
            
        # Read and verify checksum
        with open(filepath, 'rb') as f:
            data = f.read()
            calculated_hash = hashlib.sha256(data).hexdigest()
            
        return calculated_hash == metadata['sha256']
        
    async def process_continuous(self, batch_size: int = 10):
        """
        Continuously process new blocks as they arrive.
        Airplane mode: process in batches for efficiency.
        """
        print("✈️ Starting continuous block processing from cruising altitude")
        
        last_processed = None
        
        while True:
            try:
                current_block = self.w3.eth.block_number
                
                if last_processed is None:
                    # Start from current - look back 100 blocks
                    last_processed = current_block - 100
                    
                if current_block - last_processed >= batch_size:
                    # Process new batch
                    saved = self.process_block_range(
                        last_processed + 1,
                        current_block,
                        chunk_size=batch_size
                    )
                    
                    print(f"📦 Processed {len(saved)} chunks up to block {current_block}")
                    last_processed = current_block
                    
                # Wait before checking again
                await asyncio.sleep(30)
                
            except Exception as e:
                print(f"Error in continuous processing: {e}")
                await asyncio.sleep(60)


async def main():
    """Main execution - testing from the airplane."""
    chunker = BlockChunker()
    
    print("✈️ Block Chunker initialized at cruising altitude")
    print("=" * 60)
    
    # Test with recent blocks
    latest = chunker.w3.eth.block_number
    print(f"Latest block: {latest}")
    
    # Process a small test range
    test_start = latest - 10
    test_end = latest - 1
    
    print(f"\n🧪 Testing chunk creation for blocks {test_start}-{test_end}")
    
    saved_files = chunker.process_block_range(test_start, test_end, chunk_size=5)
    
    print(f"\n✅ Created {len(saved_files)} test chunks:")
    for filename in saved_files:
        print(f"  - {filename}")
        if chunker.verify_chunk(filename):
            print(f"    ✓ Verified")
        else:
            print(f"    ✗ Verification failed")
            
    print("\n🎯 Block chunker ready for production use!")
    

if __name__ == "__main__":
    asyncio.run(main())