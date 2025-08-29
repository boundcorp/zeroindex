"""
Management command to test sync monitoring and history recording.
"""
from django.core.management.base import BaseCommand
from django.utils import timezone
from zeroindex.apps.nodes.models import Node, SyncStatusHistory
from zeroindex.apps.nodes.sync_monitor import NodeSyncMonitor
import asyncio


class Command(BaseCommand):
    help = 'Test sync status history recording'

    def add_arguments(self, parser):
        parser.add_argument(
            '--trigger-workflow',
            action='store_true',
            help='Trigger the Hatchet workflow instead of running locally'
        )
        parser.add_argument(
            '--cleanup',
            action='store_true',
            help='Clean up old records'
        )
        parser.add_argument(
            '--show-history',
            action='store_true',
            help='Show recent sync history'
        )

    def handle(self, *args, **options):
        if options['trigger_workflow']:
            self.trigger_workflow()
        elif options['cleanup']:
            self.cleanup_old_records()
        elif options['show_history']:
            self.show_history()
        else:
            self.capture_sync_status()

    def capture_sync_status(self):
        """Capture sync status for all active nodes"""
        self.stdout.write("Capturing sync status for all active nodes...")
        
        active_nodes = Node.objects.filter(
            status__in=['syncing', 'running', 'provisioning']
        ).select_related('chain')
        
        self.stdout.write(f"Found {active_nodes.count()} active nodes")
        
        monitor = NodeSyncMonitor()
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        captured_count = 0
        failed_count = 0
        
        try:
            for node in active_nodes:
                self.stdout.write(f"Processing {node.name}...")
                
                try:
                    success = loop.run_until_complete(monitor.monitor_node(node))
                    
                    if success:
                        node.refresh_from_db()
                        
                        # Create history record
                        history = SyncStatusHistory.objects.create(
                            node=node,
                            execution_sync_progress=node.execution_sync_progress,
                            consensus_sync_progress=node.consensus_sync_progress,
                            current_block_height=node.current_block_height,
                            consensus_head_slot=node.consensus_head_slot,
                            node_status=node.status,
                            is_syncing=(node.status == 'syncing'),
                            metadata={}
                        )
                        
                        captured_count += 1
                        self.stdout.write(
                            self.style.SUCCESS(
                                f"✓ {node.name}: exec={node.execution_sync_progress:.1f}%, "
                                f"cons={node.consensus_sync_progress:.1f}%"
                            )
                        )
                    else:
                        failed_count += 1
                        self.stdout.write(
                            self.style.ERROR(f"✗ Failed to capture status for {node.name}")
                        )
                        
                except Exception as e:
                    failed_count += 1
                    self.stdout.write(
                        self.style.ERROR(f"✗ Error for {node.name}: {e}")
                    )
                    
                    # Still create a record with error
                    SyncStatusHistory.objects.create(
                        node=node,
                        execution_sync_progress=node.execution_sync_progress,
                        consensus_sync_progress=node.consensus_sync_progress,
                        current_block_height=node.current_block_height,
                        consensus_head_slot=node.consensus_head_slot,
                        node_status=node.status,
                        is_syncing=(node.status == 'syncing'),
                        metadata={'error': str(e)}
                    )
                    
        finally:
            loop.close()
        
        self.stdout.write(
            self.style.SUCCESS(
                f"\nCompleted: {captured_count} captured, {failed_count} failed"
            )
        )

    def cleanup_old_records(self):
        """Clean up old sync history records"""
        self.stdout.write("Cleaning up records older than 30 days...")
        
        deleted_count = SyncStatusHistory.cleanup_old_records(days=30)
        
        self.stdout.write(
            self.style.SUCCESS(f"Deleted {deleted_count} old records")
        )

    def show_history(self):
        """Show recent sync history"""
        recent_history = SyncStatusHistory.objects.select_related('node').order_by(
            '-timestamp'
        )[:20]
        
        if not recent_history:
            self.stdout.write("No sync history records found")
            return
        
        self.stdout.write("\nRecent Sync History:")
        self.stdout.write("-" * 80)
        
        for record in recent_history:
            self.stdout.write(
                f"{record.timestamp:%Y-%m-%d %H:%M} | "
                f"{record.node.name:20} | "
                f"Exec: {record.execution_sync_progress:6.2f}% | "
                f"Cons: {record.consensus_sync_progress:6.2f}% | "
                f"Block: {record.current_block_height or 0:,}"
            )
        
        # Show summary stats
        from django.db.models import Avg, Count
        
        stats = SyncStatusHistory.objects.filter(
            timestamp__gte=timezone.now() - timezone.timedelta(hours=24)
        ).aggregate(
            avg_exec=Avg('execution_sync_progress'),
            avg_cons=Avg('consensus_sync_progress'),
            total=Count('id')
        )
        
        self.stdout.write("-" * 80)
        self.stdout.write(
            f"24h Stats: {stats['total']} records, "
            f"Avg Exec: {stats['avg_exec'] or 0:.1f}%, "
            f"Avg Cons: {stats['avg_cons'] or 0:.1f}%"
        )

    def trigger_workflow(self):
        """Trigger the Hatchet workflow"""
        try:
            from zeroindex.workflows.sync_monitoring import trigger_sync_monitoring
            
            self.stdout.write("Triggering sync monitoring workflow...")
            result = trigger_sync_monitoring()
            self.stdout.write(
                self.style.SUCCESS(f"Workflow triggered: {result}")
            )
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f"Failed to trigger workflow: {e}")
            )