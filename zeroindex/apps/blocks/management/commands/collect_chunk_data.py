from django.core.management.base import BaseCommand
from django.utils import timezone
from zeroindex.apps.blocks.models import Chunk
from web3 import Web3
import json
import gzip
import os
from datetime import datetime


class Command(BaseCommand):
    help = 'Collect block data for a specific chunk'

    def add_arguments(self, parser):
        parser.add_argument('chunk_id', type=int, help='Chunk ID to collect data for')
        parser.add_argument('--batch-size', type=int, default=100, help='Number of blocks to process in each batch')
        
    def handle(self, *args, **options):
        try:
            chunk = Chunk.objects.get(id=options['chunk_id'])
        except Chunk.DoesNotExist:
            self.stdout.write(self.style.ERROR(f'Chunk with ID {options["chunk_id"]} not found'))
            return
            
        self.stdout.write(f'Collecting data for chunk: {chunk}')
        
        # Get RPC connection
        from zeroindex.apps.nodes.models import Node
        node = Node.objects.filter(
            chain=chunk.chain,
            status__in=['running', 'syncing'],
            execution_rpc_url__isnull=False
        ).first()
        
        if not node:
            self.stdout.write(self.style.ERROR('No available node found for this chain'))
            return
            
        w3 = Web3(Web3.HTTPProvider(node.execution_rpc_url))
        if not w3.is_connected():
            self.stdout.write(self.style.ERROR(f'Cannot connect to node RPC: {node.execution_rpc_url}'))
            return
            
        self.stdout.write(f'Connected to node: {node.name}')
        
        # Update chunk status
        chunk.status = 'creating'
        chunk.save()
        
        # Prepare data structure
        chunk_data = {
            'metadata': {
                'chain': chunk.chain.name,
                'chain_id': chunk.chain.chain_id,
                'start_block': chunk.start_block,
                'end_block': chunk.end_block,
                'created_at': timezone.now().isoformat(),
            },
            'blocks': []
        }
        
        total_blocks = chunk.end_block - chunk.start_block + 1
        processed_blocks = 0
        total_transactions = 0
        
        # Process blocks in batches
        for block_num in range(chunk.start_block, chunk.end_block + 1):
            try:
                self.stdout.write(f'Processing block {block_num:,} ({processed_blocks+1}/{total_blocks})')
                
                block = w3.eth.get_block(block_num, full_transactions=True)
                
                # Helper function to convert HexBytes and other Web3 objects to JSON-serializable types
                def to_json_serializable(obj):
                    if hasattr(obj, 'hex'):
                        return obj.hex()
                    elif isinstance(obj, int):
                        return obj
                    elif obj is None:
                        return None
                    else:
                        return str(obj)
                
                # Convert block to our format
                block_data = {
                    'number': block['number'],
                    'hash': to_json_serializable(block['hash']),
                    'parent_hash': to_json_serializable(block['parentHash']),
                    'timestamp': block['timestamp'],
                    'miner': block.get('miner', ''),
                    'gas_limit': block['gasLimit'],
                    'gas_used': block['gasUsed'],
                    'base_fee_per_gas': to_json_serializable(block.get('baseFeePerGas')),
                    'transaction_count': len(block['transactions']),
                    'transactions': []
                }
                
                # Add transactions
                for tx in block['transactions']:
                    tx_data = {
                        'hash': to_json_serializable(tx['hash']),
                        'from': tx['from'],
                        'to': tx.get('to'),
                        'value': str(tx['value']),
                        'gas': tx['gas'],
                        'gas_price': to_json_serializable(tx.get('gasPrice')),
                        'max_fee_per_gas': to_json_serializable(tx.get('maxFeePerGas')),
                        'max_priority_fee_per_gas': to_json_serializable(tx.get('maxPriorityFeePerGas')),
                        'nonce': tx['nonce'],
                        'transaction_index': tx['transactionIndex'],
                        'input': to_json_serializable(tx['input'])
                    }
                    block_data['transactions'].append(tx_data)
                
                chunk_data['blocks'].append(block_data)
                processed_blocks += 1
                total_transactions += len(block['transactions'])
                
                # Show progress every 100 blocks
                if processed_blocks % 100 == 0:
                    progress = (processed_blocks / total_blocks) * 100
                    self.stdout.write(f'Progress: {progress:.1f}% ({processed_blocks:,}/{total_blocks:,} blocks, {total_transactions:,} transactions)')
                
            except Exception as e:
                self.stdout.write(self.style.ERROR(f'Error processing block {block_num}: {e}'))
                continue
        
        # Save chunk data to file
        os.makedirs('data/chunks', exist_ok=True)
        file_path = f'data/chunks/chunk_{chunk.id}_{chunk.start_block}_{chunk.end_block}.json.gz'
        
        with gzip.open(file_path, 'wt') as f:
            json.dump(chunk_data, f, indent=2)
        
        # Update chunk record
        chunk.file_path = file_path
        chunk.total_blocks = processed_blocks
        chunk.total_transactions = total_transactions
        chunk.completeness_percentage = (processed_blocks / total_blocks) * 100
        chunk.status = 'complete' if processed_blocks == total_blocks else 'incomplete'
        chunk.file_size_bytes = os.path.getsize(file_path)
        
        # Calculate compression ratio
        with open(file_path.replace('.gz', ''), 'w') as f:
            json.dump(chunk_data, f, indent=2)
        uncompressed_size = os.path.getsize(file_path.replace('.gz', ''))
        chunk.compression_ratio = uncompressed_size / chunk.file_size_bytes if chunk.file_size_bytes > 0 else 1.0
        os.remove(file_path.replace('.gz', ''))  # Clean up uncompressed file
        
        chunk.save()
        
        self.stdout.write(self.style.SUCCESS(f'Chunk collection complete!'))
        self.stdout.write(f'  Status: {chunk.status}')
        self.stdout.write(f'  Blocks: {chunk.total_blocks:,}/{total_blocks:,} ({chunk.completeness_percentage:.2f}%)')
        self.stdout.write(f'  Transactions: {chunk.total_transactions:,}')
        self.stdout.write(f'  File: {chunk.file_path}')
        self.stdout.write(f'  Size: {chunk.file_size_bytes:,} bytes')
        self.stdout.write(f'  Compression: {chunk.compression_ratio:.2f}x')