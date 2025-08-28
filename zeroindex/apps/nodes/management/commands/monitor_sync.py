import asyncio
import signal
import sys
from django.core.management.base import BaseCommand, CommandError
from zeroindex.apps.nodes.sync_monitor import SyncMonitorService, monitor_node_sync, monitor_all_nodes


class Command(BaseCommand):
    help = 'Monitor sync progress of Ethereum nodes'

    def add_arguments(self, parser):
        parser.add_argument(
            '--node',
            type=str,
            help='Monitor specific node (one-time check)'
        )
        parser.add_argument(
            '--once',
            action='store_true',
            help='Run monitoring once and exit'
        )
        parser.add_argument(
            '--interval',
            type=int,
            default=60,
            help='Monitoring interval in seconds (default: 60)'
        )
        parser.add_argument(
            '--daemon',
            action='store_true',
            help='Run as daemon (continuous monitoring)'
        )

    def handle(self, *args, **options):
        node_name = options.get('node')
        once = options['once']
        interval = options['interval']
        daemon = options['daemon']
        
        if node_name:
            # Monitor specific node once
            self.monitor_single_node(node_name)
        elif once:
            # Monitor all nodes once
            self.monitor_once()
        elif daemon:
            # Run continuous monitoring
            self.run_daemon(interval)
        else:
            # Default: monitor all nodes once
            self.monitor_once()

    def monitor_single_node(self, node_name):
        """Monitor a specific node once"""
        self.stdout.write(f"Monitoring node '{node_name}'...")
        
        result = monitor_node_sync(node_name)
        
        if 'error' in result:
            raise CommandError(result['error'])
        
        self.stdout.write(
            self.style.SUCCESS(f"Successfully monitored node '{node_name}'")
        )
        
        self.stdout.write(f"Status: {result['status']}")
        self.stdout.write(f"Execution sync: {result['execution_sync_progress']:.1f}%")
        
        if result['consensus_sync_progress'] is not None:
            self.stdout.write(f"Consensus sync: {result['consensus_sync_progress']:.1f}%")
        
        if result['current_block_height']:
            self.stdout.write(f"Current block: {result['current_block_height']:,}")
        
        if result['consensus_head_slot']:
            self.stdout.write(f"Consensus slot: {result['consensus_head_slot']:,}")
        
        if result['last_health_check']:
            self.stdout.write(f"Last check: {result['last_health_check']}")

    def monitor_once(self):
        """Monitor all nodes once"""
        self.stdout.write("Monitoring all active nodes...")
        
        result = monitor_all_nodes()
        
        if 'error' in result:
            raise CommandError(result['error'])
        
        self.stdout.write(
            self.style.SUCCESS(f"Successfully monitored {result['monitored_nodes']} nodes")
        )
        
        summary = result['summary']
        self.stdout.write(f"Syncing: {summary['syncing']}")
        self.stdout.write(f"Running: {summary['running']}")

    def run_daemon(self, interval):
        """Run continuous monitoring daemon"""
        self.stdout.write(f"Starting sync monitoring daemon (interval: {interval}s)")
        self.stdout.write("Press Ctrl+C to stop")
        
        # Setup signal handlers for graceful shutdown
        service = SyncMonitorService(interval)
        
        def signal_handler(signum, frame):
            self.stdout.write("\nReceived shutdown signal, stopping...")
            service.stop()
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Run the service
        try:
            asyncio.run(service.start())
        except KeyboardInterrupt:
            self.stdout.write("\nStopping sync monitor daemon")
        except Exception as e:
            raise CommandError(f"Error running sync monitor daemon: {e}")