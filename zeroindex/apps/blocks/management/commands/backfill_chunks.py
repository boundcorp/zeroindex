from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
from django.utils import timezone
from datetime import datetime, timedelta, date
from web3 import Web3
import json
import gzip
from pathlib import Path
import time
from decimal import Decimal

from zeroindex.apps.blocks.models import Chunk, ChunkRepairLog
from zeroindex.apps.chains.models import Chain
from zeroindex.apps.nodes.models import Node


class Command(BaseCommand):
    help = 'Backfill blockchain chunks with complete validation'

    def add_arguments(self, parser):
        parser.add_argument(
            '--start-date',
            type=str,
            help='Start date (YYYY-MM-DD). Defaults to 7 days ago.'
        )
        parser.add_argument(
            '--end-date',
            type=str,
            help='End date (YYYY-MM-DD). Defaults to yesterday.'
        )
        parser.add_argument(
            '--chain-id',
            type=int,
            default=1,
            help='Chain ID to process (default: 1 for Ethereum)'
        )
        parser.add_argument(
            '--batch-size',
            type=int,
            default=100,
            help='Blocks to process in each batch'
        )
        parser.add_argument(
            '--force',
            action='store_true',
            help='Overwrite existing chunks'
        )
        parser.add_argument(
            '--validate-only',
            action='store_true',
            help='Only validate existing chunks, don\'t create new ones'
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be done without doing it'
        )

    def handle(self, *args, **options):
        self.setup_dates(options)
        self.setup_blockchain_connection(options['chain_id'])
        
        if not self.w3 or not self.w3.is_connected():
            raise CommandError('Cannot connect to blockchain node')
        
        # Calculate date ranges and block ranges
        self.calculate_block_ranges()
        
        if options['validate_only']:
            self.validate_existing_chunks()
        else:
            self.process_chunk_backfill(options)

    def setup_dates(self, options):
        """Setup start and end dates"""
        if options['start_date']:
            try:
                self.start_date = datetime.strptime(options['start_date'], '%Y-%m-%d').date()
            except ValueError:
                raise CommandError('Start date must be in YYYY-MM-DD format')
        else:
            self.start_date = (timezone.now() - timedelta(days=7)).date()
        
        if options['end_date']:
            try:
                self.end_date = datetime.strptime(options['end_date'], '%Y-%m-%d').date()
            except ValueError:
                raise CommandError('End date must be in YYYY-MM-DD format')
        else:
            self.end_date = (timezone.now() - timedelta(days=1)).date()
        
        if self.start_date > self.end_date:
            raise CommandError('Start date must be before end date')
        
        self.total_days = (self.end_date - self.start_date).days + 1
        self.stdout.write(f'📅 Processing {self.total_days} days: {self.start_date} to {self.end_date}')

    def setup_blockchain_connection(self, chain_id):
        """Setup Web3 connection to blockchain node"""
        try:
            self.chain = Chain.objects.get(chain_id=chain_id)
        except Chain.DoesNotExist:
            raise CommandError(f'Chain with ID {chain_id} not found')
        
        # Find an active node for this chain
        node = Node.objects.filter(
            chain=self.chain,
            status__in=['running', 'syncing'],
            execution_rpc_url__isnull=False
        ).first()
        
        if not node:
            raise CommandError(f'No active node found for chain {self.chain.name}')
        
        self.stdout.write(f'🔗 Using node: {node.name} ({node.execution_rpc_url})')
        self.w3 = Web3(Web3.HTTPProvider(node.execution_rpc_url))
        
        if not self.w3.is_connected():
            # Try the service endpoint if the stored one fails
            service_url = 'http://eth-mainnet-01-execution-service.devbox.svc.cluster.local:8545'
            self.stdout.write(f'🔄 Trying service endpoint: {service_url}')
            self.w3 = Web3(Web3.HTTPProvider(service_url))

    def calculate_block_ranges(self):
        """Calculate block ranges for each day"""
        if not self.w3.is_connected():
            raise CommandError('Cannot connect to get latest block')
        
        # Get current blockchain state
        latest_block = self.w3.eth.get_block('latest')
        self.latest_block_number = latest_block['number']
        self.latest_timestamp = latest_block['timestamp']
        
        self.stdout.write(f'📊 Latest block: {self.latest_block_number:,} (timestamp: {self.latest_timestamp})')
        
        # Check if we're still syncing
        sync_info = self.w3.eth.syncing
        if sync_info:
            self.stdout.write(f'⚠️  Node is syncing: {sync_info[\"currentBlock\"]:,}/{sync_info[\"highestBlock\"]:,}')
        
        # Estimate blocks per day (Ethereum averages ~7200 blocks/day)
        self.blocks_per_day = 7200
        
        # Calculate block ranges for each day
        self.day_ranges = []
        current_block = self.latest_block_number
        
        for i in range(self.total_days):
            chunk_date = self.end_date - timedelta(days=i)
            start_block = current_block - self.blocks_per_day + 1
            end_block = current_block
            
            if start_block < 0:
                start_block = 0
            
            self.day_ranges.append({
                'date': chunk_date,
                'start_block': start_block,
                'end_block': end_block,
                'expected_blocks': end_block - start_block + 1
            })
            
            current_block = start_block - 1
        
        # Reverse to chronological order
        self.day_ranges.reverse()
        
        self.stdout.write(f'📋 Calculated ranges for {len(self.day_ranges)} days:')
        for day_range in self.day_ranges:
            self.stdout.write(
                f'  {day_range[\"date\"]}: blocks {day_range[\"start_block\"]:,} - {day_range[\"end_block\"]:,} '
                f'({day_range[\"expected_blocks\"]:,} blocks)'
            )

    def validate_existing_chunks(self):
        """Validate existing chunks for completeness"""
        self.stdout.write('🔍 Validating existing chunks...')
        
        for day_range in self.day_ranges:
            chunk_date = day_range['date']
            expected_blocks = day_range['expected_blocks']
            
            chunks = Chunk.objects.filter(
                chain=self.chain,
                chunk_date=chunk_date
            ).order_by('-completeness_percentage')
            
            if not chunks.exists():
                self.stdout.write(f'❌ {chunk_date}: No chunk found')
                continue
            
            chunk = chunks.first()
            if chunks.count() > 1:
                self.stdout.write(f'⚠️  {chunk_date}: {chunks.count()} chunks found, validating best one')
            
            # Validate chunk file exists and is readable
            if not chunk.file_path or not Path(chunk.file_path).exists():
                self.stdout.write(f'❌ {chunk_date}: Chunk file missing: {chunk.file_path}')
                continue
            
            try:
                with gzip.open(chunk.file_path, 'rt') as f:
                    chunk_data = json.load(f)
                
                blocks = chunk_data.get('blocks', [])
                actual_blocks = len(blocks)
                
                # Validate block sequence
                missing_blocks = self.find_missing_blocks(
                    blocks, day_range['start_block'], day_range['end_block']
                )
                
                if missing_blocks:
                    self.stdout.write(
                        f'❌ {chunk_date}: {len(missing_blocks)} missing blocks: '
                        f'{missing_blocks[:5]}{"..." if len(missing_blocks) > 5 else ""}'
                    )
                    # Update database with missing block info
                    chunk.missing_blocks = missing_blocks
                    chunk.completeness_percentage = Decimal(
                        ((expected_blocks - len(missing_blocks)) / expected_blocks) * 100
                    )
                    chunk.status = 'incomplete' if missing_blocks else 'complete'
                    chunk.save()
                else:
                    self.stdout.write(f'✅ {chunk_date}: Complete ({actual_blocks:,} blocks)')
                    chunk.completeness_percentage = Decimal('100.00')
                    chunk.status = 'complete'
                    chunk.save()
            
            except Exception as e:
                self.stdout.write(f'❌ {chunk_date}: Error reading chunk: {str(e)}')

    def find_missing_blocks(self, blocks, start_block, end_block):
        """Find missing blocks in a chunk"""
        if not blocks:
            return list(range(start_block, end_block + 1))
        
        # Get block numbers from chunk data
        block_numbers = {int(block['number']) for block in blocks}
        expected_numbers = set(range(start_block, end_block + 1))
        
        return sorted(expected_numbers - block_numbers)

    def process_chunk_backfill(self, options):
        """Process chunk backfill for all date ranges"""
        self.stdout.write(f'🚀 Starting backfill process...')
        
        batch_size = options['batch_size']
        force = options['force']
        dry_run = options['dry_run']
        
        success_count = 0
        error_count = 0
        
        for day_range in self.day_ranges:
            chunk_date = day_range['date']
            start_block = day_range['start_block']
            end_block = day_range['end_block']
            
            try:
                # Check if chunk already exists
                existing_chunk = Chunk.objects.filter(
                    chain=self.chain,
                    chunk_date=chunk_date
                ).first()
                
                if existing_chunk and not force:
                    if existing_chunk.completeness_percentage == 100:
                        self.stdout.write(f'⏭️  {chunk_date}: Complete chunk exists, skipping')
                        success_count += 1
                        continue
                    else:
                        self.stdout.write(f'🔧 {chunk_date}: Incomplete chunk exists, repairing...')
                
                if dry_run:
                    self.stdout.write(f'🏃 {chunk_date}: Would process {start_block:,} - {end_block:,}')
                    continue
                
                # Create/update chunk
                chunk = self.create_or_update_chunk(chunk_date, start_block, end_block)
                
                # Collect block data
                self.collect_chunk_data(chunk, start_block, end_block, batch_size)
                
                # Validate completeness
                missing_blocks = chunk.analyze_missing_blocks()
                if missing_blocks:
                    self.stdout.write(f'⚠️  {chunk_date}: {len(missing_blocks)} blocks missing')
                else:
                    self.stdout.write(f'✅ {chunk_date}: Complete chunk created')
                
                success_count += 1
                
            except Exception as e:
                self.stdout.write(f'❌ {chunk_date}: Error - {str(e)}')
                error_count += 1
        
        # Summary
        total = success_count + error_count
        self.stdout.write(f'\\n📊 Backfill complete: {success_count}/{total} chunks processed successfully')
        if error_count > 0:
            self.stdout.write(f'⚠️  {error_count} chunks had errors')

    def create_or_update_chunk(self, chunk_date, start_block, end_block):
        """Create or update chunk record"""
        chunk, created = Chunk.objects.get_or_create(
            chain=self.chain,
            chunk_date=chunk_date,
            defaults={
                'start_block': start_block,
                'end_block': end_block,
                'status': 'creating',
                'completeness_percentage': Decimal('0.00'),
                'missing_blocks': [],
                'total_blocks': 0,
                'total_transactions': 0,
            }
        )
        
        if not created:
            # Update existing chunk
            chunk.start_block = start_block
            chunk.end_block = end_block
            chunk.status = 'creating'
            chunk.save()
        
        # Ensure file path is set
        if not chunk.file_path:
            file_path = Path('data/chunks') / f'chunk_{chunk_date}_{start_block}_{end_block}.json.gz'
            file_path.parent.mkdir(parents=True, exist_ok=True)
            chunk.file_path = str(file_path)
            chunk.save()
        
        return chunk

    def collect_chunk_data(self, chunk, start_block, end_block, batch_size):
        """Collect blockchain data for a chunk"""
        self.stdout.write(f'📦 Collecting data for {chunk.chunk_date}: blocks {start_block:,} - {end_block:,}')
        
        blocks = []
        total_transactions = 0
        
        for batch_start in range(start_block, end_block + 1, batch_size):
            batch_end = min(batch_start + batch_size - 1, end_block)
            
            self.stdout.write(f'  Processing batch: {batch_start:,} - {batch_end:,}')
            
            for block_num in range(batch_start, batch_end + 1):
                try:
                    block = self.w3.eth.get_block(block_num, full_transactions=True)
                    
                    # Convert block to our JSON format
                    block_data = {
                        'number': block['number'],
                        'hash': block['hash'].hex(),
                        'parent_hash': block['parentHash'].hex(),
                        'timestamp': block['timestamp'],
                        'miner': block.get('miner', ''),
                        'difficulty': str(block.get('difficulty', 0)),
                        'total_difficulty': str(block.get('totalDifficulty', 0)),
                        'gas_limit': block['gasLimit'],
                        'gas_used': block['gasUsed'],
                        'base_fee_per_gas': block.get('baseFeePerGas'),
                        'transaction_count': len(block['transactions']),
                        'transactions_root': block.get('transactionsRoot', '').hex() if block.get('transactionsRoot') else '',
                        'state_root': block.get('stateRoot', '').hex() if block.get('stateRoot') else '',
                        'receipts_root': block.get('receiptsRoot', '').hex() if block.get('receiptsRoot') else '',
                        'size': block.get('size', 0),
                        'extra_data': block.get('extraData', '').hex() if block.get('extraData') else '',
                        'transactions': []
                    }
                    
                    # Add transaction data
                    for tx in block['transactions']:
                        tx_data = {
                            'hash': tx['hash'].hex(),
                            'transaction_index': tx['transactionIndex'],
                            'from': tx['from'],
                            'to': tx.get('to', ''),
                            'value': str(tx['value']),
                            'gas': tx['gas'],
                            'gas_price': str(tx.get('gasPrice', 0)),
                            'max_fee_per_gas': str(tx.get('maxFeePerGas', 0)) if tx.get('maxFeePerGas') else None,
                            'max_priority_fee_per_gas': str(tx.get('maxPriorityFeePerGas', 0)) if tx.get('maxPriorityFeePerGas') else None,
                            'nonce': tx['nonce'],
                            'input': tx.get('input', '').hex() if tx.get('input') else '',
                            'transaction_type': tx.get('type', 0),
                            'chain_id': tx.get('chainId'),
                        }
                        block_data['transactions'].append(tx_data)
                    
                    blocks.append(block_data)
                    total_transactions += len(block['transactions'])
                    
                except Exception as e:
                    self.stdout.write(f'    ❌ Error fetching block {block_num}: {str(e)}')
                    # Continue with other blocks
                    continue
            
            # Save progress periodically
            if len(blocks) % (batch_size * 5) == 0:
                self.save_chunk_data(chunk, blocks, total_transactions, partial=True)
        
        # Save final data
        self.save_chunk_data(chunk, blocks, total_transactions, partial=False)
        
        self.stdout.write(f'✅ Collected {len(blocks):,} blocks, {total_transactions:,} transactions')

    def save_chunk_data(self, chunk, blocks, total_transactions, partial=False):
        """Save chunk data to compressed JSON file"""
        chunk_data = {
            'chunk_date': chunk.chunk_date.isoformat(),
            'start_block': chunk.start_block,
            'end_block': chunk.end_block,
            'chain_id': chunk.chain.chain_id,
            'chain_name': chunk.chain.name,
            'total_blocks': len(blocks),
            'total_transactions': total_transactions,
            'created_at': chunk.created_at.isoformat(),
            'updated_at': timezone.now().isoformat(),
            'is_partial': partial,
            'blocks': blocks
        }
        
        # Save to compressed file
        file_path = Path(chunk.file_path)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        with gzip.open(file_path, 'wt') as f:
            json.dump(chunk_data, f, indent=2)
        
        # Update chunk record
        chunk.total_blocks = len(blocks)
        chunk.total_transactions = total_transactions
        chunk.file_size_bytes = file_path.stat().st_size
        chunk.status = 'creating' if partial else 'complete'
        chunk.updated_at = timezone.now()
        
        if not partial:
            # Calculate completeness
            expected_blocks = chunk.end_block - chunk.start_block + 1
            chunk.completeness_percentage = Decimal((len(blocks) / expected_blocks) * 100)
        
        chunk.save()