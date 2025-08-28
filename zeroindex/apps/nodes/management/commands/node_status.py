from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone
from datetime import timedelta
from zeroindex.apps.nodes.models import Node
from zeroindex.apps.nodes.services import KubernetesNodeManager


class Command(BaseCommand):
    help = 'Check status of Ethereum nodes'

    def add_arguments(self, parser):
        parser.add_argument(
            'node_name',
            nargs='?',
            type=str,
            help='Name of specific node to check (optional)'
        )
        parser.add_argument(
            '--all',
            action='store_true',
            help='Show status of all nodes'
        )
        parser.add_argument(
            '--sync-only',
            action='store_true',
            help='Only show nodes that are syncing'
        )
        parser.add_argument(
            '--format',
            choices=['table', 'json'],
            default='table',
            help='Output format (default: table)'
        )

    def handle(self, *args, **options):
        node_name = options.get('node_name')
        show_all = options['all']
        sync_only = options['sync_only']
        output_format = options['format']
        
        if node_name:
            # Show status for specific node
            try:
                node = Node.objects.get(name=node_name)
                self.show_single_node_status(node, output_format)
            except Node.DoesNotExist:
                raise CommandError(f'Node "{node_name}" does not exist')
        elif show_all or sync_only:
            # Show status for multiple nodes
            nodes = Node.objects.all()
            if sync_only:
                nodes = nodes.filter(status='syncing')
            self.show_multiple_nodes_status(nodes, output_format)
        else:
            # Show summary
            self.show_summary()

    def show_single_node_status(self, node, output_format):
        """Show detailed status for a single node"""
        if output_format == 'json':
            import json
            status_data = self.get_node_status_data(node)
            self.stdout.write(json.dumps(status_data, indent=2, default=str))
            return
        
        # Table format
        self.stdout.write(f"\n{self.style.HTTP_INFO('=== Node Status: ' + node.name + ' ===')}")
        self.stdout.write(f"Chain: {node.chain.name} (ID: {node.chain.chain_id})")
        self.stdout.write(f"Type: {node.get_node_type_display()}")
        self.stdout.write(f"Status: {self.colorize_status(node.status)}")
        
        if node.is_ethereum_l1:
            self.stdout.write(f"Execution Client: {node.execution_client} {node.execution_version}")
            self.stdout.write(f"Consensus Client: {node.consensus_client} {node.consensus_version}")
        else:
            self.stdout.write(f"Client: {node.execution_client} {node.execution_version}")
        
        # Sync progress
        if node.is_ethereum_l1:
            self.stdout.write(f"Execution Sync: {node.execution_sync_progress:.1f}%")
            self.stdout.write(f"Consensus Sync: {node.consensus_sync_progress:.1f}%")
            self.stdout.write(f"Overall Progress: {node.overall_sync_progress:.1f}%")
        else:
            self.stdout.write(f"Sync Progress: {node.execution_sync_progress:.1f}%")
        
        # Block height info
        if node.current_block_height:
            self.stdout.write(f"Current Block: {node.current_block_height:,}")
        
        if node.consensus_head_slot and node.is_ethereum_l1:
            self.stdout.write(f"Consensus Slot: {node.consensus_head_slot:,}")
        
        # Kubernetes status
        if node.kube_credential and node.kube_credential.is_active:
            try:
                k8s_manager = KubernetesNodeManager(node.kube_credential)
                k8s_status = k8s_manager.get_node_status(node)
                self.show_k8s_status(k8s_status)
                self.show_storage_status(k8s_status.get('storage', {}))
            except Exception as e:
                self.stdout.write(f"K8s Status: {self.style.ERROR('Error: ' + str(e))}")
        
        # Timestamps
        if node.last_health_check:
            time_since = timezone.now() - node.last_health_check
            self.stdout.write(f"Last Health Check: {self.format_timedelta(time_since)} ago")
        
        self.stdout.write(f"Created: {node.created_at.strftime('%Y-%m-%d %H:%M:%S')}")
        self.stdout.write(f"Updated: {node.updated_at.strftime('%Y-%m-%d %H:%M:%S')}")

    def show_multiple_nodes_status(self, nodes, output_format):
        """Show status for multiple nodes"""
        if output_format == 'json':
            import json
            data = [self.get_node_status_data(node) for node in nodes]
            self.stdout.write(json.dumps(data, indent=2, default=str))
            return
        
        # Table format
        self.stdout.write(f"\n{self.style.HTTP_INFO('=== Nodes Status ===')}")
        self.stdout.write(f"{'Name':<20} {'Chain':<12} {'Status':<12} {'Sync %':<8} {'Block':<12}")
        self.stdout.write("-" * 70)
        
        for node in nodes:
            sync_pct = f"{node.overall_sync_progress:.1f}%"
            block_height = f"{node.current_block_height:,}" if node.current_block_height else "N/A"
            
            self.stdout.write(
                f"{node.name:<20} "
                f"{node.chain.name:<12} "
                f"{self.colorize_status(node.status):<20} "  # Extra space for color codes
                f"{sync_pct:<8} "
                f"{block_height:<12}"
            )

    def show_summary(self):
        """Show summary of all nodes"""
        nodes = Node.objects.all()
        total = nodes.count()
        
        if total == 0:
            self.stdout.write("No nodes found.")
            return
        
        status_counts = {}
        for status_key, _ in Node.STATUS_CHOICES:
            count = nodes.filter(status=status_key).count()
            if count > 0:
                status_counts[status_key] = count
        
        self.stdout.write(f"\n{self.style.HTTP_INFO('=== Nodes Summary ===')}")
        self.stdout.write(f"Total nodes: {total}")
        
        for status, count in status_counts.items():
            self.stdout.write(f"{status.title()}: {count}")
        
        # Recent activity
        recent_nodes = nodes.filter(
            updated_at__gte=timezone.now() - timedelta(hours=1)
        ).order_by('-updated_at')[:5]
        
        if recent_nodes:
            self.stdout.write(f"\n{self.style.HTTP_INFO('Recent Activity:')}")
            for node in recent_nodes:
                time_ago = self.format_timedelta(timezone.now() - node.updated_at)
                self.stdout.write(f"  {node.name}: {node.status} ({time_ago} ago)")

    def show_k8s_status(self, k8s_status):
        """Show Kubernetes deployment status"""
        self.stdout.write(f"\n{self.style.HTTP_INFO('Kubernetes Status:')}")
        
        exec_status = k8s_status.get('execution_client')
        if exec_status:
            if 'error' in exec_status:
                self.stdout.write(f"  Execution: {self.style.ERROR(exec_status['error'])}")
            else:
                ready = exec_status.get('ready_replicas', 0)
                total = exec_status.get('replicas', 0)
                self.stdout.write(f"  Execution: {ready}/{total} replicas ready")
        
        cons_status = k8s_status.get('consensus_client')
        if cons_status:
            if 'error' in cons_status:
                self.stdout.write(f"  Consensus: {self.style.ERROR(cons_status['error'])}")
            else:
                ready = cons_status.get('ready_replicas', 0)
                total = cons_status.get('replicas', 0)
                self.stdout.write(f"  Consensus: {ready}/{total} replicas ready")

    def show_storage_status(self, storage_status):
        """Show PVC storage usage status"""
        if not storage_status:
            return
            
        self.stdout.write(f"\n{self.style.HTTP_INFO('Storage Status:')}")
        
        for storage_type, info in storage_status.items():
            if 'error' in info:
                self.stdout.write(f"  {storage_type.title()}: {self.style.ERROR(info['error'])}")
                continue
                
            pvc_name = info.get('pvc_name', storage_type)
            capacity = info.get('capacity', 'Unknown')
            usage_pct = info.get('usage_percentage', 0)
            used_bytes = info.get('used_bytes', 0)
            status = info.get('status', 'Unknown')
            
            # Format used space in human readable format
            used_human = self.format_bytes(used_bytes)
            
            # Color code usage percentage
            usage_color = self.get_usage_color(usage_pct)
            usage_display = usage_color(f"{usage_pct}%")
            
            self.stdout.write(
                f"  {storage_type.title()}: {used_human} / {capacity} "
                f"({usage_display}) - {pvc_name} [{status}]"
            )

    def get_node_status_data(self, node):
        """Get node status data for JSON output"""
        data = {
            'name': node.name,
            'chain': {
                'name': node.chain.name,
                'chain_id': node.chain.chain_id,
            },
            'status': node.status,
            'node_type': node.node_type,
            'execution_client': node.execution_client,
            'consensus_client': node.consensus_client if node.is_ethereum_l1 else None,
            'sync_progress': {
                'execution': node.execution_sync_progress,
                'consensus': node.consensus_sync_progress if node.is_ethereum_l1 else None,
                'overall': node.overall_sync_progress,
            },
            'current_block_height': node.current_block_height,
            'consensus_head_slot': node.consensus_head_slot,
            'is_fully_synced': node.is_fully_synced,
            'last_health_check': node.last_health_check,
            'created_at': node.created_at,
            'updated_at': node.updated_at,
        }
        
        # Add Kubernetes status if available
        if node.kube_credential and node.kube_credential.is_active:
            try:
                k8s_manager = KubernetesNodeManager(node.kube_credential)
                k8s_status = k8s_manager.get_node_status(node)
                data['kubernetes'] = k8s_status
                data['storage'] = k8s_status.get('storage', {})
            except Exception as e:
                data['kubernetes'] = {'error': str(e)}
                data['storage'] = {'error': str(e)}
        
        return data

    def colorize_status(self, status):
        """Colorize status based on value"""
        color_map = {
            'running': self.style.SUCCESS,
            'syncing': self.style.WARNING,
            'provisioning': self.style.HTTP_INFO,
            'error': self.style.ERROR,
            'stopped': self.style.HTTP_NOT_MODIFIED,
            'pending': self.style.NOTICE,
        }
        
        color_func = color_map.get(status, lambda x: x)
        return color_func(status)

    def format_timedelta(self, td):
        """Format timedelta for display"""
        if td.days > 0:
            return f"{td.days}d {td.seconds//3600}h"
        elif td.seconds > 3600:
            return f"{td.seconds//3600}h {(td.seconds%3600)//60}m"
        elif td.seconds > 60:
            return f"{td.seconds//60}m"
        else:
            return f"{td.seconds}s"

    def format_bytes(self, bytes_value):
        """Format bytes in human readable format"""
        if bytes_value == 0:
            return "0 B"
        
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_value < 1024.0:
                return f"{bytes_value:.1f} {unit}"
            bytes_value /= 1024.0
        return f"{bytes_value:.1f} PB"

    def get_usage_color(self, usage_pct):
        """Get color function based on usage percentage"""
        if usage_pct >= 90:
            return self.style.ERROR
        elif usage_pct >= 75:
            return self.style.WARNING
        elif usage_pct >= 50:
            return self.style.NOTICE
        else:
            return self.style.SUCCESS