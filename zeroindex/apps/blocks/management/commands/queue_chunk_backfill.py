from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
from django.utils import timezone
from datetime import datetime, timedelta, date
from decimal import Decimal
import time

from zeroindex.apps.blocks.models import Chunk
from zeroindex.apps.chains.models import Chain
from zeroindex.apps.blocks.tasks import (
    process_chunk_task, 
    validate_chunk_task, 
    upload_chunk_to_s3_task
)


class Command(BaseCommand):
    help = 'Queue chunk backfill tasks using Celery for parallel processing'

    def add_arguments(self, parser):
        parser.add_argument(
            '--start-date',
            type=str,
            help='Start date (YYYY-MM-DD). Defaults to 7 days ago.'
        )
        parser.add_argument(
            '--days',
            type=int,
            default=7,
            help='Number of days to process'
        )
        parser.add_argument(
            '--chain-id',
            type=int,
            default=1,
            help='Chain ID to process (default: 1 for Ethereum)'
        )
        parser.add_argument(
            '--force',
            action='store_true',
            help='Reprocess existing chunks'
        )
        parser.add_argument(
            '--upload',
            action='store_true',
            help='Upload to S3 after processing'
        )

    def handle(self, *args, **options):
        self.setup_dates(options)
        self.setup_chain(options['chain_id'])
        
        self.stdout.write('🚀 Starting complete blockchain backfill process...')
        
        # Queue the processing tasks
        self.queue_processing_tasks(options)

    def setup_dates(self, options):
        """Setup date range"""
        days = options['days']
        if options['start_date']:
            try:
                self.start_date = datetime.strptime(options['start_date'], '%Y-%m-%d').date()
            except ValueError:
                raise CommandError('Start date must be in YYYY-MM-DD format')
        else:
            self.start_date = (timezone.now() - timedelta(days=days)).date()
        
        self.end_date = self.start_date + timedelta(days=days-1)
        self.total_days = days
        
        self.stdout.write(f'📅 Processing {self.total_days} days: {self.start_date} to {self.end_date}')

    def setup_chain(self, chain_id):
        """Setup blockchain chain"""
        try:
            self.chain = Chain.objects.get(chain_id=chain_id)
        except Chain.DoesNotExist:
            raise CommandError(f'Chain with ID {chain_id} not found')
        
        self.stdout.write(f'⛓️  Processing chain: {self.chain.name}')

    def queue_processing_tasks(self, options):
        """Queue chunk processing tasks with real blockchain data"""
        self.stdout.write('📦 Creating chunk processing pipeline...')
        
        force = options['force']
        upload_after = options['upload']
        
        # Use our comprehensive backfill command for the heavy lifting
        current_date = self.start_date
        chunks_queued = 0
        
        while current_date <= self.end_date:
            # Check if chunk exists and is complete
            existing_chunk = Chunk.objects.filter(
                chain=self.chain,
                chunk_date=current_date
            ).first()
            
            if existing_chunk and not force:
                if existing_chunk.completeness_percentage >= 99.0:
                    self.stdout.write(f'⏭️  {current_date}: Complete chunk exists, skipping')
                    current_date += timedelta(days=1)
                    continue
            
            # Queue the backfill command for this specific day
            self.stdout.write(f'📋 Queuing backfill for {current_date}')
            
            # We'll use the synchronous backfill command since it's comprehensive
            from django.core.management import call_command
            try:
                call_command(
                    'backfill_chunks',
                    start_date=current_date.strftime('%Y-%m-%d'),
                    end_date=current_date.strftime('%Y-%m-%d'),
                    chain_id=self.chain.chain_id,
                    batch_size=100
                )
                
                chunks_queued += 1
                self.stdout.write(f'✅ Completed backfill for {current_date}')
                
                # Upload to S3 if requested
                if upload_after:
                    self.stdout.write(f'⬆️  Uploading {current_date} to S3...')
                    call_command(
                        'upload_chunks_to_s3',
                        date=current_date.strftime('%Y-%m-%d'),
                        days=1
                    )
                
            except Exception as e:
                self.stdout.write(f'❌ Error processing {current_date}: {str(e)}')
            
            current_date += timedelta(days=1)
        
        self.stdout.write(f'🎉 Completed processing {chunks_queued} days of blockchain data')