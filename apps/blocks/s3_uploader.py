#!/usr/bin/env python3
"""
S3 uploader for blockchain chunks.
Uploading to the cloud from the clouds - meta recursive storage.
"""

import os
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import json
import hashlib
import asyncio
from concurrent.futures import ThreadPoolExecutor
import time


class S3Uploader:
    """
    Handles uploading compressed block chunks to S3.
    Optimized for bandwidth efficiency and resumable uploads.
    """
    
    def __init__(self, bucket_name: str, 
                 aws_access_key_id: Optional[str] = None,
                 aws_secret_access_key: Optional[str] = None,
                 region: Optional[str] = None,
                 prefix: str = 'ethereum/blocks'):
        """Initialize S3 client with credentials."""
        
        self.bucket_name = bucket_name
        self.prefix = prefix.rstrip('/')
        
        # Initialize S3 client
        session = boto3.Session(
            aws_access_key_id=aws_access_key_id or os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=aws_secret_access_key or os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=region or os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
        )
        
        self.s3 = session.client('s3')
        self.bucket_exists = False
        
    def check_credentials(self) -> bool:
        """Verify S3 credentials and bucket access."""
        try:
            self.s3.head_bucket(Bucket=self.bucket_name)
            self.bucket_exists = True
            return True
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                print(f"Bucket {self.bucket_name} not found")
            elif error_code == '403':
                print(f"Access denied to bucket {self.bucket_name}")
            else:
                print(f"Error accessing bucket: {e}")
            return False
        except NoCredentialsError:
            print("AWS credentials not found")
            return False
            
    def generate_s3_key(self, filename: str, timestamp: Optional[int] = None) -> str:
        """
        Generate S3 key with hierarchical structure for efficient querying.
        Structure: prefix/year/month/day/hour/filename
        """
        if timestamp is None:
            # Extract timestamp from filename if possible
            parts = filename.split('_')
            if len(parts) >= 5:
                try:
                    timestamp = int(parts[-1].split('.')[0])
                except ValueError:
                    timestamp = int(time.time())
            else:
                timestamp = int(time.time())
                
        dt = datetime.fromtimestamp(timestamp)
        
        key = f"{self.prefix}/{dt.year:04d}/{dt.month:02d}/{dt.day:02d}/{dt.hour:02d}/{filename}"
        return key
        
    def upload_file(self, local_path: Path, s3_key: Optional[str] = None,
                   metadata: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """
        Upload a single file to S3 with metadata.
        Returns upload result information.
        """
        if not self.bucket_exists and not self.check_credentials():
            raise ValueError("Cannot access S3 bucket")
            
        s3_key = s3_key or self.generate_s3_key(local_path.name)
        
        # Calculate file hash for integrity
        with open(local_path, 'rb') as f:
            file_data = f.read()
            file_hash = hashlib.sha256(file_data).hexdigest()
            file_size = len(file_data)
            
        # Prepare metadata
        upload_metadata = {
            'upload_time': datetime.utcnow().isoformat(),
            'file_size': str(file_size),
            'sha256': file_hash,
            'source': 'zeroindex-blocks',
            'uploaded_from': 'airplane-wifi'  # Because why not
        }
        
        if metadata:
            upload_metadata.update(metadata)
            
        try:
            # Upload with metadata
            self.s3.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=file_data,
                Metadata=upload_metadata,
                ContentType='application/gzip',
                ContentEncoding='gzip'
            )
            
            return {
                'success': True,
                's3_key': s3_key,
                'file_size': file_size,
                'sha256': file_hash,
                'uploaded_at': upload_metadata['upload_time']
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                's3_key': s3_key,
                'file_size': file_size
            }
            
    def upload_chunk_batch(self, chunk_files: List[Path], 
                          max_workers: int = 4) -> List[Dict[str, Any]]:
        """
        Upload multiple chunk files concurrently.
        Limited concurrency to respect airplane wifi bandwidth.
        """
        print(f"✈️ Uploading {len(chunk_files)} chunks via airplane wifi...")
        
        results = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Create upload tasks
            future_to_file = {
                executor.submit(self.upload_file, chunk_file): chunk_file
                for chunk_file in chunk_files
            }
            
            # Process completed uploads
            for future in future_to_file:
                chunk_file = future_to_file[future]
                try:
                    result = future.result()
                    result['local_file'] = str(chunk_file)
                    results.append(result)
                    
                    if result['success']:
                        print(f"📤 Uploaded {chunk_file.name} -> {result['s3_key']}")
                    else:
                        print(f"❌ Failed {chunk_file.name}: {result['error']}")
                        
                except Exception as e:
                    results.append({
                        'success': False,
                        'error': str(e),
                        'local_file': str(chunk_file)
                    })
                    
        return results
        
    def list_uploaded_chunks(self, date_prefix: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        List chunks already uploaded to S3.
        date_prefix format: 'YYYY/MM/DD' or 'YYYY/MM' or 'YYYY'
        """
        prefix = self.prefix
        if date_prefix:
            prefix = f"{prefix}/{date_prefix}"
            
        try:
            paginator = self.s3.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)
            
            chunks = []
            for page in page_iterator:
                for obj in page.get('Contents', []):
                    if obj['Key'].endswith('.json.gz'):
                        # Get metadata
                        try:
                            head_response = self.s3.head_object(
                                Bucket=self.bucket_name,
                                Key=obj['Key']
                            )
                            metadata = head_response.get('Metadata', {})
                        except Exception:
                            metadata = {}
                            
                        chunks.append({
                            'key': obj['Key'],
                            'size': obj['Size'],
                            'last_modified': obj['LastModified'],
                            'etag': obj['ETag'],
                            'metadata': metadata
                        })
                        
            return chunks
            
        except Exception as e:
            print(f"Error listing chunks: {e}")
            return []
            
    def verify_upload(self, s3_key: str, expected_hash: str) -> bool:
        """Verify uploaded file matches expected hash."""
        try:
            response = self.s3.head_object(Bucket=self.bucket_name, Key=s3_key)
            uploaded_hash = response.get('Metadata', {}).get('sha256', '')
            return uploaded_hash == expected_hash
        except Exception:
            return False
            
    def download_chunk(self, s3_key: str, local_path: Path) -> bool:
        """Download a chunk from S3 for verification."""
        try:
            self.s3.download_file(self.bucket_name, s3_key, str(local_path))
            return True
        except Exception as e:
            print(f"Download failed: {e}")
            return False
            
    def calculate_storage_stats(self, date_prefix: Optional[str] = None) -> Dict[str, Any]:
        """Calculate storage statistics for uploaded chunks."""
        chunks = self.list_uploaded_chunks(date_prefix)
        
        if not chunks:
            return {'chunk_count': 0, 'total_size': 0}
            
        total_size = sum(chunk['size'] for chunk in chunks)
        oldest = min(chunk['last_modified'] for chunk in chunks)
        newest = max(chunk['last_modified'] for chunk in chunks)
        
        return {
            'chunk_count': len(chunks),
            'total_size': total_size,
            'total_size_mb': total_size / (1024 * 1024),
            'total_size_gb': total_size / (1024 * 1024 * 1024),
            'date_range': {
                'oldest': oldest.isoformat(),
                'newest': newest.isoformat()
            },
            'average_chunk_size': total_size / len(chunks)
        }
        
    async def sync_directory(self, chunks_dir: Path, 
                           check_existing: bool = True) -> Dict[str, Any]:
        """
        Sync entire chunks directory to S3.
        Optionally skip files that already exist.
        """
        print(f"✈️ Syncing {chunks_dir} to S3 bucket {self.bucket_name}")
        
        # Find all chunk files
        chunk_files = list(chunks_dir.glob('*.json.gz'))
        
        if check_existing:
            # Get list of already uploaded files
            existing = self.list_uploaded_chunks()
            existing_names = set(Path(chunk['key']).name for chunk in existing)
            
            # Filter out already uploaded
            chunk_files = [f for f in chunk_files if f.name not in existing_names]
            
        if not chunk_files:
            print("📦 All chunks already uploaded")
            return {'uploaded': 0, 'skipped': 0, 'errors': 0}
            
        # Upload in batches to manage bandwidth
        batch_size = 10  # Smaller batches for airplane wifi
        results = {'uploaded': 0, 'skipped': 0, 'errors': 0}
        
        for i in range(0, len(chunk_files), batch_size):
            batch = chunk_files[i:i + batch_size]
            
            print(f"📤 Uploading batch {i//batch_size + 1} ({len(batch)} files)")
            batch_results = self.upload_chunk_batch(batch, max_workers=2)
            
            for result in batch_results:
                if result['success']:
                    results['uploaded'] += 1
                else:
                    results['errors'] += 1
                    
            # Brief pause between batches to be nice to airplane wifi
            if i + batch_size < len(chunk_files):
                print("⏸️  Brief pause for airplane wifi stability...")
                await asyncio.sleep(5)
                
        return results


async def main():
    """Test S3 uploader from the airplane."""
    
    # Note: This will only work with proper AWS credentials
    bucket_name = os.getenv('S3_BLOCKS_BUCKET', 'zeroindex-blocks-test')
    
    uploader = S3Uploader(bucket_name)
    
    print("✈️ S3 Uploader initialized at cruising altitude")
    print("=" * 60)
    
    # Check credentials (will fail without real creds)
    print("🔑 Checking S3 credentials...")
    if uploader.check_credentials():
        print("✅ S3 credentials valid")
        
        # List any existing chunks
        chunks = uploader.list_uploaded_chunks()
        print(f"📦 Found {len(chunks)} existing chunks")
        
        # Calculate storage stats
        stats = uploader.calculate_storage_stats()
        print(f"💾 Total storage: {stats.get('total_size_mb', 0):.2f} MB")
        
    else:
        print("❌ S3 credentials not configured")
        print("Set AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and S3_BLOCKS_BUCKET")
        
    print("\n🎯 S3 uploader ready for production use!")
    print("Remember to configure AWS credentials before uploading")


if __name__ == "__main__":
    asyncio.run(main())