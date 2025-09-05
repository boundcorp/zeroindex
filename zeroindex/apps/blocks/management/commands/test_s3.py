from django.core.management.base import BaseCommand
from django.conf import settings
import boto3
from botocore.exceptions import ClientError
import json


class Command(BaseCommand):
    help = 'Test S3 connectivity and permissions'

    def handle(self, *args, **options):
        self.stdout.write('Testing AWS S3 connectivity...')
        
        # Check if credentials are configured
        if not settings.AWS_ACCESS_KEY_ID or not settings.AWS_SECRET_ACCESS_KEY:
            self.stdout.write(
                self.style.ERROR('AWS credentials not configured in environment')
            )
            return
        
        self.stdout.write(f'Using bucket: {settings.AWS_S3_BUCKET_NAME}')
        self.stdout.write(f'Region: {settings.AWS_S3_REGION_NAME}')
        
        try:
            # Create S3 client
            s3_client = boto3.client(
                's3',
                aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
                aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
                region_name=settings.AWS_S3_REGION_NAME
            )
            
            # Test bucket access
            self.stdout.write('Testing bucket access...')
            response = s3_client.head_bucket(Bucket=settings.AWS_S3_BUCKET_NAME)
            self.stdout.write(self.style.SUCCESS('✓ Bucket access successful'))
            
            # Test write permissions
            self.stdout.write('Testing write permissions...')
            test_key = 'test/connectivity-test.json'
            test_data = json.dumps({
                'test': True,
                'timestamp': '2025-01-01T00:00:00Z',
                'message': 'S3 connectivity test successful'
            })
            
            s3_client.put_object(
                Bucket=settings.AWS_S3_BUCKET_NAME,
                Key=test_key,
                Body=test_data,
                ContentType='application/json'
            )
            self.stdout.write(self.style.SUCCESS(f'✓ Write test successful: {test_key}'))
            
            # Test read permissions
            self.stdout.write('Testing read permissions...')
            response = s3_client.get_object(
                Bucket=settings.AWS_S3_BUCKET_NAME,
                Key=test_key
            )
            data = response['Body'].read().decode('utf-8')
            parsed_data = json.loads(data)
            
            if parsed_data.get('test'):
                self.stdout.write(self.style.SUCCESS('✓ Read test successful'))
            else:
                self.stdout.write(self.style.ERROR('✗ Read test failed - invalid data'))
            
            # Test list permissions
            self.stdout.write('Testing list permissions...')
            response = s3_client.list_objects_v2(
                Bucket=settings.AWS_S3_BUCKET_NAME,
                Prefix='test/',
                MaxKeys=10
            )
            
            if 'Contents' in response:
                self.stdout.write(self.style.SUCCESS(f'✓ List test successful - found {len(response["Contents"])} objects'))
            else:
                self.stdout.write(self.style.SUCCESS('✓ List test successful - no objects found'))
            
            # Clean up test file
            self.stdout.write('Cleaning up test file...')
            s3_client.delete_object(
                Bucket=settings.AWS_S3_BUCKET_NAME,
                Key=test_key
            )
            self.stdout.write(self.style.SUCCESS('✓ Cleanup successful'))
            
            self.stdout.write(
                self.style.SUCCESS('\n🎉 All S3 connectivity tests passed!')
            )
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            self.stdout.write(
                self.style.ERROR(f'✗ AWS ClientError: {error_code} - {error_message}')
            )
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f'✗ Unexpected error: {str(e)}')
            )