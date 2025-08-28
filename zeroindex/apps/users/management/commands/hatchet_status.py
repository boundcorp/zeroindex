"""
Django management command to check Hatchet configuration and status.
"""
from django.core.management.base import BaseCommand
from zeroindex.utils.hatchet import is_hatchet_configured, get_hatchet_config


class Command(BaseCommand):
    help = 'Check Hatchet configuration and connection status'

    def handle(self, *args, **options):
        self.stdout.write('Checking Hatchet configuration...')
        
        config = get_hatchet_config()
        
        self.stdout.write('\n=== Hatchet Configuration ===')
        for key, value in config.items():
            if key == 'token_configured':
                self.stdout.write(f'{key}: {value}')
            else:
                self.stdout.write(f'{key}: {value}')
        
        if is_hatchet_configured():
            self.stdout.write(
                self.style.SUCCESS('\n✓ Hatchet appears to be configured correctly')
            )
            
            # Try to create a client to test connection
            try:
                from zeroindex.utils.hatchet import get_hatchet_client
                hatchet = get_hatchet_client()
                self.stdout.write(
                    self.style.SUCCESS('✓ Hatchet client created successfully')
                )
            except Exception as e:
                self.stdout.write(
                    self.style.ERROR(f'✗ Failed to create Hatchet client: {e}')
                )
        else:
            self.stdout.write(
                self.style.ERROR('\n✗ Hatchet is not properly configured')
            )
            self.stdout.write('Please ensure HATCHET_SERVER_URL and HATCHET_CLIENT_TOKEN are set')
        
        self.stdout.write('\n=== Next Steps ===')
        self.stdout.write('1. Make sure your Hatchet server is running at the configured URL')
        self.stdout.write('2. Generate a client token from your Hatchet admin interface')
        self.stdout.write('3. Start the worker with: ./manage.py hatchet_worker')
        self.stdout.write('4. Trigger workflows using the utilities in zeroindex.utils.workflows')