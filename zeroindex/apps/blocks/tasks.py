from celery import shared_task
from django.utils import timezone
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
import json
import gzip
import logging

from .models import Chunk
from zeroindex.apps.chains.models import Chain
from zeroindex.apps.nodes.models import Node
from web3 import Web3

logger = logging.getLogger(__name__)


@shared_task(bind=True, max_retries=3)
def process_chunk_task(self, chunk_id, start_block, end_block, batch_size=100):
    """
    Celery task to process a single chunk of blockchain data
    """
    try:
        chunk = Chunk.objects.get(id=chunk_id)
        logger.info(f"Processing chunk {chunk_id}: {start_block} - {end_block}")
        
        # Get Web3 connection
        w3 = get_web3_connection(chunk.chain)
        if not w3:
            raise Exception("Cannot connect to blockchain node")
        
        # Process the chunk
        blocks, total_transactions = collect_blocks_for_range(
            w3, start_block, end_block, batch_size
        )
        
        # Save chunk data
        save_chunk_data(chunk, blocks, total_transactions)
        
        # Update chunk status
        expected_blocks = end_block - start_block + 1
        chunk.total_blocks = len(blocks)
        chunk.total_transactions = total_transactions
        chunk.completeness_percentage = Decimal((len(blocks) / expected_blocks) * 100)
        chunk.status = 'complete' if len(blocks) == expected_blocks else 'incomplete'
        chunk.updated_at = timezone.now()
        chunk.save()
        
        logger.info(f"Completed chunk {chunk_id}: {len(blocks)} blocks collected")
        
        return {
            'chunk_id': chunk_id,
            'blocks_collected': len(blocks),
            'transactions_collected': total_transactions,
            'completeness': float(chunk.completeness_percentage),
            'status': chunk.status
        }
        
    except Exception as exc:
        logger.error(f"Error processing chunk {chunk_id}: {str(exc)}")
        
        # Update chunk status to error
        try:
            chunk = Chunk.objects.get(id=chunk_id)
            chunk.status = 'failed'
            chunk.save()
        except:
            pass
        
        # Retry the task
        if self.request.retries < self.max_retries:
            raise self.retry(countdown=60 * (2 ** self.request.retries), exc=exc)
        
        raise exc


@shared_task
def validate_chunk_task(chunk_id):
    """
    Celery task to validate a chunk's completeness
    """
    try:
        chunk = Chunk.objects.get(id=chunk_id)
        logger.info(f"Validating chunk {chunk_id} for {chunk.chunk_date}")
        
        if not chunk.file_path or not Path(chunk.file_path).exists():
            chunk.status = 'failed'
            chunk.save()
            return {'chunk_id': chunk_id, 'status': 'failed', 'error': 'File not found'}
        
        # Read and validate chunk data
        with gzip.open(chunk.file_path, 'rt') as f:
            chunk_data = json.load(f)
        
        blocks = chunk_data.get('blocks', [])
        missing_blocks = find_missing_blocks_in_range(
            blocks, chunk.start_block, chunk.end_block
        )
        
        # Update chunk with validation results
        expected_blocks = chunk.end_block - chunk.start_block + 1
        actual_blocks = len(blocks)
        
        chunk.missing_blocks = missing_blocks
        chunk.total_blocks = actual_blocks
        chunk.completeness_percentage = Decimal(
            ((actual_blocks - len(missing_blocks)) / expected_blocks) * 100
        )
        chunk.status = 'complete' if not missing_blocks else 'incomplete'
        chunk.updated_at = timezone.now()
        chunk.save()
        
        result = {
            'chunk_id': chunk_id,
            'date': chunk.chunk_date.isoformat(),
            'expected_blocks': expected_blocks,
            'actual_blocks': actual_blocks,
            'missing_blocks': len(missing_blocks),
            'completeness': float(chunk.completeness_percentage),
            'status': chunk.status
        }
        
        logger.info(f"Validated chunk {chunk_id}: {result}")
        return result
        
    except Exception as exc:
        logger.error(f"Error validating chunk {chunk_id}: {str(exc)}")
        return {'chunk_id': chunk_id, 'status': 'error', 'error': str(exc)}


@shared_task
def upload_chunk_to_s3_task(chunk_id):
    """
    Celery task to upload a chunk to S3
    """
    try:
        from django.core.management import call_command
        from io import StringIO
        
        chunk = Chunk.objects.get(id=chunk_id)
        
        # Use our existing upload command
        out = StringIO()
        call_command(
            'upload_chunks_to_s3',
            date=chunk.chunk_date.isoformat(),
            days=1,
            stdout=out
        )
        
        result = out.getvalue()
        logger.info(f"S3 upload result for chunk {chunk_id}: {result}")
        
        return {
            'chunk_id': chunk_id,
            'date': chunk.chunk_date.isoformat(),
            'upload_result': result
        }
        
    except Exception as exc:
        logger.error(f"Error uploading chunk {chunk_id} to S3: {str(exc)}")
        return {'chunk_id': chunk_id, 'status': 'error', 'error': str(exc)}


def get_web3_connection(chain):
    """Get Web3 connection for a chain"""
    node = Node.objects.filter(
        chain=chain,
        status__in=['running', 'syncing'],
        execution_rpc_url__isnull=False
    ).first()
    
    if not node:
        # Try service endpoint as fallback
        if chain.chain_id == 1:  # Ethereum
            service_url = 'http://eth-mainnet-01-execution-service.devbox.svc.cluster.local:8545'
            w3 = Web3(Web3.HTTPProvider(service_url))
            if w3.is_connected():
                return w3
        return None
    
    w3 = Web3(Web3.HTTPProvider(node.execution_rpc_url))
    return w3 if w3.is_connected() else None


def collect_blocks_for_range(w3, start_block, end_block, batch_size):
    """Collect blockchain data for a block range"""
    blocks = []
    total_transactions = 0
    
    for block_num in range(start_block, end_block + 1):
        try:
            block = w3.eth.get_block(block_num, full_transactions=True)
            
            # Convert to JSON-serializable format
            block_data = {
                'number': block['number'],
                'hash': block['hash'].hex(),
                'parent_hash': block['parentHash'].hex(),
                'timestamp': block['timestamp'],
                'miner': block.get('miner', ''),
                'difficulty': str(block.get('difficulty', 0)),
                'gas_limit': block['gasLimit'],
                'gas_used': block['gasUsed'],
                'base_fee_per_gas': block.get('baseFeePerGas'),
                'transaction_count': len(block['transactions']),
                'transactions': []
            }
            
            # Add transactions
            for tx in block['transactions']:
                tx_data = {
                    'hash': tx['hash'].hex(),
                    'from': tx['from'],
                    'to': tx.get('to', ''),
                    'value': str(tx['value']),
                    'gas': tx['gas'],
                    'gas_price': str(tx.get('gasPrice', 0)),
                    'nonce': tx['nonce'],
                    'transaction_index': tx['transactionIndex']
                }
                block_data['transactions'].append(tx_data)
            
            blocks.append(block_data)
            total_transactions += len(block['transactions'])
            
        except Exception as e:
            logger.error(f"Error fetching block {block_num}: {e}")
            continue
    
    return blocks, total_transactions


def save_chunk_data(chunk, blocks, total_transactions):
    """Save chunk data to compressed file"""
    chunk_data = {
        'chunk_date': chunk.chunk_date.isoformat(),
        'start_block': chunk.start_block,
        'end_block': chunk.end_block,
        'chain_id': chunk.chain.chain_id,
        'total_blocks': len(blocks),
        'total_transactions': total_transactions,
        'created_at': chunk.created_at.isoformat(),
        'updated_at': timezone.now().isoformat(),
        'blocks': blocks
    }
    
    # Ensure file path
    if not chunk.file_path:
        file_path = Path('data/chunks') / f'chunk_{chunk.chunk_date}_{chunk.start_block}_{chunk.end_block}.json.gz'
        file_path.parent.mkdir(parents=True, exist_ok=True)
        chunk.file_path = str(file_path)
        chunk.save()
    
    file_path = Path(chunk.file_path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    
    with gzip.open(file_path, 'wt') as f:
        json.dump(chunk_data, f, indent=2)
    
    chunk.file_size_bytes = file_path.stat().st_size


def find_missing_blocks_in_range(blocks, start_block, end_block):
    """Find missing blocks in a range"""
    if not blocks:
        return list(range(start_block, end_block + 1))
    
    block_numbers = {int(block['number']) for block in blocks}
    expected_numbers = set(range(start_block, end_block + 1))
    
    return sorted(expected_numbers - block_numbers)