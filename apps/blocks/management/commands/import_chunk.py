import json
import gzip
from datetime import datetime
from django.core.management.base import BaseCommand
from zeroindex.apps.blocks.models import Chunk
from zeroindex.apps.chains.models import Chain


class Command(BaseCommand):
    help = 'Import chunk from compressed JSON file'

    def add_arguments(self, parser):
        parser.add_argument('file_path', type=str, help='Path to the chunk file')
        parser.add_argument('--chain-symbol', type=str, default='ETH', help='Chain symbol')

    def handle(self, *args, **options):
        file_path = options['file_path']
        chain_symbol = options['chain_symbol']
        
        try:
            chain = Chain.objects.get(symbol=chain_symbol)
        except Chain.DoesNotExist:
            self.stdout.write(self.style.ERROR(f'Chain {chain_symbol} not found'))
            return

        self.stdout.write(f'Loading chunk from {file_path}...')
        
        with gzip.open(file_path, 'rt') as f:
            chunk_data = json.load(f)
        
        blocks = chunk_data['blocks']
        start_block = min(int(block['number']) for block in blocks)
        end_block = max(int(block['number']) for block in blocks)
        
        # Calculate expected vs actual blocks
        expected_blocks = end_block - start_block + 1
        actual_blocks = len(blocks)
        completeness = (actual_blocks / expected_blocks) * 100 if expected_blocks > 0 else 0
        
        # Find missing blocks
        existing_block_numbers = {int(block['number']) for block in blocks}
        missing_blocks = [
            block_num for block_num in range(start_block, end_block + 1)
            if block_num not in existing_block_numbers
        ]
        
        chunk, created = Chunk.objects.update_or_create(
            chain=chain,
            start_block=start_block,
            end_block=end_block,
            defaults={
                'file_path': file_path,
                'completeness_percentage': completeness,
                'missing_blocks': missing_blocks,
                'total_blocks': actual_blocks,
                'total_transactions': sum(int(block.get('transaction_count', 0)) for block in blocks),
                'file_size_bytes': chunk_data.get('metadata', {}).get('compressed_size_mb', 0) * 1024 * 1024,
                'compression_ratio': chunk_data.get('metadata', {}).get('compression_ratio', 1.0),
                'created_at': datetime.now(),
                'updated_at': datetime.now(),
            }
        )
        
        action = "Created" if created else "Updated"
        self.stdout.write(
            self.style.SUCCESS(
                f'{action} chunk: {start_block}-{end_block} '
                f'({actual_blocks}/{expected_blocks} blocks, {completeness:.2f}% complete)'
            )
        )
        
        if missing_blocks:
            self.stdout.write(
                self.style.WARNING(f'Missing blocks: {missing_blocks}')
            )
            
            # Test repair functionality
            self.stdout.write('Testing repair functionality...')
            try:
                repair_log = chunk.repair_missing_blocks()
                if repair_log:
                    self.stdout.write(
                        self.style.SUCCESS(
                            f'Repair completed: {repair_log.blocks_attempted} attempted, '
                            f'{repair_log.blocks_repaired} repaired'
                        )
                    )
                else:
                    self.stdout.write(self.style.ERROR('Repair failed'))
            except Exception as e:
                self.stdout.write(self.style.ERROR(f'Repair error: {e}'))
        else:
            self.stdout.write(self.style.SUCCESS('Chunk is complete!'))