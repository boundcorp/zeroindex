"""
Django management command to run Hatchet worker.
"""
import logging
from django.core.management.base import BaseCommand
from zeroindex.utils.hatchet import get_hatchet_client, is_hatchet_configured

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Start Hatchet worker to process workflows'

    def add_arguments(self, parser):
        parser.add_argument(
            '--worker-name',
            type=str,
            default='django-worker',
            help='Name for the Hatchet worker (default: django-worker)'
        )

    def handle(self, *args, **options):
        if not is_hatchet_configured():
            self.stdout.write(
                self.style.ERROR(
                    'Hatchet is not configured. Please set HATCHET_CLIENT_TOKEN environment variable.'
                )
            )
            return

        worker_name = options['worker_name']
        self.stdout.write(f'Starting Hatchet worker: {worker_name}')

        try:
            hatchet = get_hatchet_client()
            worker = hatchet.worker(worker_name)
            
            # Register all workflows (will be imported automatically)
            from zeroindex.workflows import *  # noqa
            
            self.stdout.write(
                self.style.SUCCESS(f'Hatchet worker "{worker_name}" started successfully')
            )
            
            # Start the worker (this blocks)
            worker.start()
            
        except KeyboardInterrupt:
            self.stdout.write('\nShutting down Hatchet worker...')
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f'Error starting Hatchet worker: {e}')
            )
            logger.exception('Error starting Hatchet worker')