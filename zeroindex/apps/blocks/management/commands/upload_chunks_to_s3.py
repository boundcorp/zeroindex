from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
from django.utils import timezone
from datetime import datetime, timedelta
import boto3
import json
import gzip
import os
from pathlib import Path
from zeroindex.apps.blocks.models import Chunk


class Command(BaseCommand):
    help = 'Upload blockchain chunks to S3 bucket'

    def add_arguments(self, parser):
        parser.add_argument(
            '--date',
            type=str,
            help='Upload chunk for specific date (YYYY-MM-DD). Defaults to yesterday.'
        )
        parser.add_argument(
            '--days',
            type=int,
            default=1,
            help='Number of days to upload (starting from --date or yesterday)'
        )
        parser.add_argument(
            '--verify-only',
            action='store_true',
            help='Only verify chunks exist in database, do not upload'
        )
        parser.add_argument(
            '--force',
            action='store_true',
            help='Upload even if chunk already exists in S3'
        )

    def handle(self, *args, **options):
        # Parse date argument
        if options['date']:
            try:
                start_date = datetime.strptime(options['date'], '%Y-%m-%d').date()
            except ValueError:
                raise CommandError('Date must be in YYYY-MM-DD format')
        else:
            # Default to yesterday
            start_date = (timezone.now() - timedelta(days=1)).date()
        
        days_count = options['days']
        verify_only = options['verify_only']
        force_upload = options['force']
        
        self.stdout.write(f'Processing {days_count} day(s) starting from {start_date}')
        
        # Initialize S3 client
        if not verify_only:
            s3_client = boto3.client(
                's3',
                aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
                aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
                region_name=settings.AWS_S3_REGION_NAME
            )
            self.stdout.write(f'Using S3 bucket: {settings.AWS_S3_BUCKET_NAME}')
        
        success_count = 0
        error_count = 0
        
        # Process each date
        for i in range(days_count):
            current_date = start_date + timedelta(days=i)
            
            try:
                # Find chunks for this date
                chunks = Chunk.objects.filter(chunk_date=current_date).order_by('-completeness_percentage', '-updated_at')
                if not chunks.exists():
                    self.stdout.write(
                        self.style.ERROR(f'❌ {current_date}: No chunk found in database')
                    )
                    error_count += 1
                    continue
                
                # Use the most complete chunk for this date
                chunk = chunks.first()
                if chunks.count() > 1:
                    self.stdout.write(f'ℹ️  {current_date}: Found {chunks.count()} chunks, using most complete one')
                
                self.stdout.write(f'📦 {current_date}: Found chunk (blocks {chunk.start_block}-{chunk.end_block})')
                
                if verify_only:
                    # Just verify file exists and has data
                    if chunk.file_path and Path(chunk.file_path).exists():
                        try:
                            with gzip.open(chunk.file_path, 'rt') as f:
                                chunk_data = json.load(f)
                                block_count = len(chunk_data.get('blocks', []))
                            
                            self.stdout.write(
                                self.style.SUCCESS(
                                    f'✅ {current_date}: Chunk verified '
                                    f'({block_count} blocks, {chunk.completeness_percentage}% complete)'
                                )
                            )
                            success_count += 1
                        except Exception as e:
                            self.stdout.write(
                                self.style.ERROR(f'❌ {current_date}: Error reading chunk file: {str(e)}')
                            )
                            error_count += 1
                    else:
                        self.stdout.write(
                            self.style.ERROR(f'❌ {current_date}: Chunk file not found: {chunk.file_path}')
                        )
                        error_count += 1
                    continue
                
                # Prepare S3 key
                s3_key = f'chunks/{current_date.year}/{current_date.month:02d}/chunk_{current_date}.json.gz'
                
                # Check if already exists in S3
                if not force_upload:
                    try:
                        s3_client.head_object(Bucket=settings.AWS_S3_BUCKET_NAME, Key=s3_key)
                        self.stdout.write(f'⏭️  {current_date}: Already exists in S3, skipping')
                        success_count += 1
                        continue
                    except Exception as e:
                        if '404' in str(e) or 'NoSuchKey' in str(e):
                            pass  # File doesn't exist, continue with upload
                        else:
                            raise  # Re-raise if it's a different error
                
                # Check if chunk has a file
                if not chunk.file_path or not Path(chunk.file_path).exists():
                    self.stdout.write(
                        self.style.ERROR(f'❌ {current_date}: Chunk file not found: {chunk.file_path}')
                    )
                    error_count += 1
                    continue
                
                # Read the existing compressed chunk file
                chunk_file_path = Path(chunk.file_path)
                with gzip.open(chunk_file_path, 'rb') as f:
                    compressed_data = f.read()
                
                # For info, also read the uncompressed size
                with gzip.open(chunk_file_path, 'rt') as f:
                    chunk_data = json.load(f)
                    block_count = len(chunk_data.get('blocks', []))
                    json_size = len(json.dumps(chunk_data).encode('utf-8'))
                
                # Upload to S3
                s3_client.put_object(
                    Bucket=settings.AWS_S3_BUCKET_NAME,
                    Key=s3_key,
                    Body=compressed_data,
                    ContentType='application/gzip',
                    ContentEncoding='gzip',
                    Metadata={
                        'chunk-date': current_date.isoformat(),
                        'block-count': str(block_count),
                        'start-block': str(chunk.start_block),
                        'end-block': str(chunk.end_block),
                        'completeness': str(float(chunk.completeness_percentage)),
                    }
                )
                
                # Calculate compression info
                compressed_size = len(compressed_data)
                compression_ratio = (1 - compressed_size / json_size) * 100 if json_size > 0 else 0
                
                self.stdout.write(
                    self.style.SUCCESS(
                        f'✅ {current_date}: Uploaded to S3 '
                        f'({compressed_size:,} bytes, {compression_ratio:.1f}% compression) '
                        f'-> {s3_key}'
                    )
                )
                success_count += 1
                
            except Exception as e:
                self.stdout.write(
                    self.style.ERROR(f'❌ {current_date}: Error - {str(e)}')
                )
                error_count += 1
        
        # Summary
        total_processed = success_count + error_count
        if verify_only:
            self.stdout.write(
                self.style.SUCCESS(
                    f'\n📊 Verification complete: {success_count}/{total_processed} chunks verified successfully'
                )
            )
        else:
            self.stdout.write(
                self.style.SUCCESS(
                    f'\n📊 Upload complete: {success_count}/{total_processed} chunks uploaded successfully'
                )
            )
        
        if error_count > 0:
            self.stdout.write(
                self.style.WARNING(f'⚠️  {error_count} chunks had errors')
            )