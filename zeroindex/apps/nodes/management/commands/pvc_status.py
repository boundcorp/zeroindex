from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone
from datetime import timedelta
from zeroindex.apps.nodes.models import Node
from zeroindex.apps.nodes.services import KubernetesNodeManager
import json


class Command(BaseCommand):
    help = 'Investigate PVC (Persistent Volume Claim) storage usage for blockchain nodes'

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
            help='Show PVC status for all nodes'
        )
        parser.add_argument(
            '--format',
            choices=['table', 'json', 'summary'],
            default='table',
            help='Output format (default: table)'
        )
        parser.add_argument(
            '--sort-by',
            choices=['name', 'usage', 'size', 'capacity'],
            default='usage',
            help='Sort results by field (default: usage)'
        )
        parser.add_argument(
            '--threshold',
            type=int,
            default=0,
            help='Only show PVCs with usage above threshold percentage (0-100)'
        )
        parser.add_argument(
            '--include-totals',
            action='store_true',
            help='Include total storage usage across all nodes'
        )

    def handle(self, *args, **options):
        node_name = options.get('node_name')
        show_all = options['all']
        output_format = options['format']
        sort_by = options['sort_by']
        threshold = options['threshold']
        include_totals = options['include_totals']
        
        if node_name:
            # Show PVC status for specific node
            try:
                node = Node.objects.get(name=node_name)
                self.show_single_node_pvc_status(node, output_format)
            except Node.DoesNotExist:
                raise CommandError(f'Node "{node_name}" does not exist')
        elif show_all:
            # Show PVC status for all nodes
            nodes = Node.objects.filter(kube_credential__is_active=True)
            self.show_multiple_nodes_pvc_status(nodes, output_format, sort_by, threshold, include_totals)
        else:
            # Show summary of PVC usage
            self.show_pvc_summary(sort_by, threshold)

    def show_single_node_pvc_status(self, node, output_format):
        """Show detailed PVC status for a single node"""
        if not node.kube_credential or not node.kube_credential.is_active:
            raise CommandError(f'Node "{node.name}" has no active Kubernetes credentials')
        
        try:
            k8s_manager = KubernetesNodeManager(node.kube_credential)
            storage_status = k8s_manager.get_storage_status(node)
            
            if output_format == 'json':
                output_data = {
                    'node': node.name,
                    'chain': node.chain.name,
                    'storage': storage_status,
                    'timestamp': timezone.now().isoformat(),
                }
                self.stdout.write(json.dumps(output_data, indent=2, default=str))
                return
            
            # Table format
            self.stdout.write(f"\n{self.style.HTTP_INFO('=== PVC Status: ' + node.name + ' ===')}")
            self.stdout.write(f"Chain: {node.chain.name}")
            self.stdout.write(f"Node Type: {node.get_node_type_display()}")
            self.stdout.write(f"Kubernetes: {node.kube_credential.cluster_name}/{node.kube_credential.namespace}")
            
            if not storage_status:
                self.stdout.write(f"{self.style.WARNING('No PVC data available')}")
                return
            
            self.show_storage_details(storage_status)
            
        except Exception as e:
            raise CommandError(f'Failed to get PVC status: {str(e)}')

    def show_multiple_nodes_pvc_status(self, nodes, output_format, sort_by, threshold, include_totals):
        """Show PVC status for multiple nodes"""
        if not nodes.exists():
            self.stdout.write("No nodes with active Kubernetes credentials found.")
            return
        
        all_storage_data = []
        
        # Collect storage data from all nodes
        for node in nodes:
            try:
                k8s_manager = KubernetesNodeManager(node.kube_credential)
                storage_status = k8s_manager.get_storage_status(node)
                
                node_data = {
                    'node': node,
                    'storage': storage_status or {},
                    'total_usage_bytes': 0,
                    'total_capacity_bytes': 0,
                }
                
                # Calculate totals for this node
                for storage_type, info in (storage_status or {}).items():
                    if 'used_bytes' in info:
                        node_data['total_usage_bytes'] += info.get('used_bytes', 0)
                    if 'capacity_bytes' in info:
                        node_data['total_capacity_bytes'] += info.get('capacity_bytes', 0)
                
                all_storage_data.append(node_data)
                
            except Exception as e:
                self.stdout.write(f"{self.style.ERROR('Error getting storage for ' + node.name + ': ' + str(e))}")
                continue
        
        if not all_storage_data:
            self.stdout.write("No storage data available.")
            return
        
        # Filter by threshold
        if threshold > 0:
            filtered_data = []
            for data in all_storage_data:
                for storage_type, info in data['storage'].items():
                    usage_pct = info.get('usage_percentage', 0)
                    if usage_pct >= threshold:
                        filtered_data.append(data)
                        break
            all_storage_data = filtered_data
        
        # Sort data
        if sort_by == 'name':
            all_storage_data.sort(key=lambda x: x['node'].name)
        elif sort_by == 'usage':
            all_storage_data.sort(key=lambda x: x['total_usage_bytes'], reverse=True)
        elif sort_by == 'size':
            all_storage_data.sort(key=lambda x: x['total_usage_bytes'], reverse=True)
        elif sort_by == 'capacity':
            all_storage_data.sort(key=lambda x: x['total_capacity_bytes'], reverse=True)
        
        if output_format == 'json':
            output_data = {
                'nodes': [{
                    'name': data['node'].name,
                    'chain': data['node'].chain.name,
                    'storage': data['storage'],
                    'total_usage_bytes': data['total_usage_bytes'],
                    'total_capacity_bytes': data['total_capacity_bytes'],
                } for data in all_storage_data],
                'timestamp': timezone.now().isoformat(),
            }
            if include_totals:
                output_data['totals'] = self.calculate_totals(all_storage_data)
            self.stdout.write(json.dumps(output_data, indent=2, default=str))
            return
        
        # Table format
        if output_format == 'summary':
            self.show_pvc_table_summary(all_storage_data, include_totals)
        else:
            self.show_pvc_table_detailed(all_storage_data, include_totals)

    def show_pvc_summary(self, sort_by, threshold):
        """Show summary of all PVC usage"""
        nodes = Node.objects.filter(kube_credential__is_active=True)
        
        if not nodes.exists():
            self.stdout.write("No nodes with active Kubernetes credentials found.")
            return
        
        self.stdout.write(f"\n{self.style.HTTP_INFO('=== PVC Storage Summary ===')}")
        
        total_nodes = nodes.count()
        total_usage = 0
        total_capacity = 0
        high_usage_count = 0
        
        storage_by_type = {}
        
        for node in nodes:
            try:
                k8s_manager = KubernetesNodeManager(node.kube_credential)
                storage_status = k8s_manager.get_storage_status(node)
                
                if not storage_status:
                    continue
                
                for storage_type, info in storage_status.items():
                    used_bytes = info.get('used_bytes', 0)
                    capacity_bytes = info.get('capacity_bytes', 0)
                    usage_pct = info.get('usage_percentage', 0)
                    
                    total_usage += used_bytes
                    total_capacity += capacity_bytes
                    
                    if usage_pct >= 75:
                        high_usage_count += 1
                    
                    if storage_type not in storage_by_type:
                        storage_by_type[storage_type] = {
                            'count': 0,
                            'total_used': 0,
                            'total_capacity': 0,
                        }
                    
                    storage_by_type[storage_type]['count'] += 1
                    storage_by_type[storage_type]['total_used'] += used_bytes
                    storage_by_type[storage_type]['total_capacity'] += capacity_bytes
                    
            except Exception as e:
                self.stdout.write(f"{self.style.ERROR('Error: ' + str(e))}")
                continue
        
        # Display summary
        self.stdout.write(f"Total Nodes: {total_nodes}")
        self.stdout.write(f"Total Storage Used: {self.format_bytes(total_usage)}")
        self.stdout.write(f"Total Storage Capacity: {self.format_bytes(total_capacity)}")
        
        if total_capacity > 0:
            overall_pct = (total_usage / total_capacity) * 100
            usage_color = self.get_usage_color(overall_pct)
            self.stdout.write(f"Overall Usage: {usage_color(f'{overall_pct:.1f}%')}")
        
        if high_usage_count > 0:
            self.stdout.write(f"{self.style.WARNING('High Usage (>75%): ' + str(high_usage_count) + ' PVCs')}")
        
        # Storage by type
        if storage_by_type:
            self.stdout.write(f"\n{self.style.HTTP_INFO('Storage by Type:')}")
            for storage_type, stats in storage_by_type.items():
                avg_usage_pct = 0
                if stats['total_capacity'] > 0:
                    avg_usage_pct = (stats['total_used'] / stats['total_capacity']) * 100
                
                self.stdout.write(
                    f"  {storage_type.title()}: {stats['count']} PVCs, "
                    f"{self.format_bytes(stats['total_used'])} / {self.format_bytes(stats['total_capacity'])} "
                    f"({avg_usage_pct:.1f}%)"
                )

    def show_pvc_table_detailed(self, all_storage_data, include_totals):
        """Show detailed table of PVC usage"""
        self.stdout.write(f"\n{self.style.HTTP_INFO('=== Detailed PVC Status ===')}")
        
        # Header
        header = f"{'Node':<20} {'Chain':<12} {'Type':<12} {'PVC Name':<25} {'Used':<10} {'Capacity':<10} {'Usage %':<8} {'Status':<10}"
        self.stdout.write(header)
        self.stdout.write("-" * len(header))
        
        total_used = 0
        total_capacity = 0
        
        for data in all_storage_data:
            node = data['node']
            storage = data['storage']
            
            if not storage:
                self.stdout.write(
                    f"{node.name:<20} {node.chain.name:<12} {'N/A':<12} {'No data':<25} "
                    f"{'N/A':<10} {'N/A':<10} {'N/A':<8} {'N/A':<10}"
                )
                continue
            
            for storage_type, info in storage.items():
                if 'error' in info:
                    self.stdout.write(
                        f"{node.name:<20} {node.chain.name:<12} {storage_type:<12} {'Error':<25} "
                        f"{'N/A':<10} {'N/A':<10} {'N/A':<8} {self.style.ERROR('Error'):<18}"
                    )
                    continue
                
                pvc_name = info.get('pvc_name', storage_type)[:24]  # Truncate long names
                used_bytes = info.get('used_bytes', 0)
                capacity_bytes = info.get('capacity_bytes', 0)
                usage_pct = info.get('usage_percentage', 0)
                status = info.get('status', 'Unknown')
                
                used_human = self.format_bytes(used_bytes)
                capacity_human = self.format_bytes(capacity_bytes)
                usage_color = self.get_usage_color(usage_pct)
                usage_display = usage_color(f"{usage_pct:.1f}%")
                
                total_used += used_bytes
                total_capacity += capacity_bytes
                
                # Handle color codes in formatting (add extra space)
                status_display = status[:9]  # Truncate status
                usage_padded = f"{usage_display:<16}"  # Extra padding for color codes
                
                self.stdout.write(
                    f"{node.name:<20} {node.chain.name:<12} {storage_type:<12} {pvc_name:<25} "
                    f"{used_human:<10} {capacity_human:<10} {usage_padded} {status_display:<10}"
                )
        
        if include_totals and total_capacity > 0:
            overall_usage_pct = (total_used / total_capacity) * 100
            usage_color = self.get_usage_color(overall_usage_pct)
            self.stdout.write("-" * len(header))
            self.stdout.write(
                f"{'TOTALS':<20} {'':<12} {'':<12} {'':<25} "
                f"{self.format_bytes(total_used):<10} {self.format_bytes(total_capacity):<10} "
                f"{usage_color(f'{overall_usage_pct:.1f}%'):<16} {'':<10}"
            )

    def show_pvc_table_summary(self, all_storage_data, include_totals):
        """Show summary table of PVC usage by node"""
        self.stdout.write(f"\n{self.style.HTTP_INFO('=== PVC Summary by Node ===')}")
        
        # Header
        header = f"{'Node':<20} {'Chain':<12} {'PVCs':<5} {'Total Used':<12} {'Total Capacity':<15} {'Avg Usage %':<12}"
        self.stdout.write(header)
        self.stdout.write("-" * len(header))
        
        grand_total_used = 0
        grand_total_capacity = 0
        total_pvcs = 0
        
        for data in all_storage_data:
            node = data['node']
            storage = data['storage']
            
            if not storage:
                self.stdout.write(f"{node.name:<20} {node.chain.name:<12} {'0':<5} {'N/A':<12} {'N/A':<15} {'N/A':<12}")
                continue
            
            node_used = 0
            node_capacity = 0
            pvc_count = len(storage)
            
            for storage_type, info in storage.items():
                if 'used_bytes' in info:
                    node_used += info.get('used_bytes', 0)
                if 'capacity_bytes' in info:
                    node_capacity += info.get('capacity_bytes', 0)
            
            avg_usage_pct = 0
            if node_capacity > 0:
                avg_usage_pct = (node_used / node_capacity) * 100
            
            usage_color = self.get_usage_color(avg_usage_pct)
            
            self.stdout.write(
                f"{node.name:<20} {node.chain.name:<12} {pvc_count:<5} "
                f"{self.format_bytes(node_used):<12} {self.format_bytes(node_capacity):<15} "
                f"{usage_color(f'{avg_usage_pct:.1f}%'):<20}"
            )
            
            grand_total_used += node_used
            grand_total_capacity += node_capacity
            total_pvcs += pvc_count
        
        if include_totals and grand_total_capacity > 0:
            overall_usage_pct = (grand_total_used / grand_total_capacity) * 100
            usage_color = self.get_usage_color(overall_usage_pct)
            self.stdout.write("-" * len(header))
            self.stdout.write(
                f"{'TOTALS':<20} {'':<12} {total_pvcs:<5} "
                f"{self.format_bytes(grand_total_used):<12} {self.format_bytes(grand_total_capacity):<15} "
                f"{usage_color(f'{overall_usage_pct:.1f}%'):<20}"
            )

    def show_storage_details(self, storage_status):
        """Show detailed storage information for a single node"""
        self.stdout.write(f"\n{self.style.HTTP_INFO('Storage Details:')}")
        
        for storage_type, info in storage_status.items():
            self.stdout.write(f"\n  {self.style.SUCCESS(storage_type.title() + ' Storage:')}")
            
            if 'error' in info:
                self.stdout.write(f"    Error: {self.style.ERROR(info['error'])}")
                continue
            
            pvc_name = info.get('pvc_name', 'Unknown')
            namespace = info.get('namespace', 'Unknown')
            storage_class = info.get('storage_class', 'Unknown')
            capacity = info.get('capacity', 'Unknown')
            used_bytes = info.get('used_bytes', 0)
            available_bytes = info.get('available_bytes', 0)
            usage_pct = info.get('usage_percentage', 0)
            status = info.get('status', 'Unknown')
            
            self.stdout.write(f"    PVC Name: {pvc_name}")
            self.stdout.write(f"    Namespace: {namespace}")
            self.stdout.write(f"    Storage Class: {storage_class}")
            self.stdout.write(f"    Capacity: {capacity}")
            self.stdout.write(f"    Used: {self.format_bytes(used_bytes)}")
            self.stdout.write(f"    Available: {self.format_bytes(available_bytes)}")
            
            usage_color = self.get_usage_color(usage_pct)
            self.stdout.write(f"    Usage: {usage_color(f'{usage_pct:.1f}%')}")
            self.stdout.write(f"    Status: {status}")

    def calculate_totals(self, all_storage_data):
        """Calculate total storage statistics"""
        total_used = 0
        total_capacity = 0
        total_pvcs = 0
        storage_types = {}
        
        for data in all_storage_data:
            for storage_type, info in data['storage'].items():
                if 'used_bytes' in info:
                    used = info.get('used_bytes', 0)
                    capacity = info.get('capacity_bytes', 0)
                    
                    total_used += used
                    total_capacity += capacity
                    total_pvcs += 1
                    
                    if storage_type not in storage_types:
                        storage_types[storage_type] = {'used': 0, 'capacity': 0, 'count': 0}
                    
                    storage_types[storage_type]['used'] += used
                    storage_types[storage_type]['capacity'] += capacity
                    storage_types[storage_type]['count'] += 1
        
        return {
            'total_used_bytes': total_used,
            'total_capacity_bytes': total_capacity,
            'total_pvcs': total_pvcs,
            'overall_usage_percentage': (total_used / total_capacity * 100) if total_capacity > 0 else 0,
            'storage_by_type': storage_types,
        }

    def format_bytes(self, bytes_value):
        """Format bytes in human readable format"""
        if bytes_value == 0:
            return "0 B"
        
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_value < 1024.0:
                return f"{bytes_value:.1f}{unit}"
            bytes_value /= 1024.0
        return f"{bytes_value:.1f}PB"

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