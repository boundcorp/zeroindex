from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from zeroindex.apps.nodes.models import Node
from zeroindex.apps.nodes.services import KubernetesNodeManager


class Command(BaseCommand):
    help = 'Stop an Ethereum node deployment'

    def add_arguments(self, parser):
        parser.add_argument(
            'node_name',
            type=str,
            help='Name of the node to stop'
        )
        parser.add_argument(
            '--keep-data',
            action='store_true',
            help='Keep persistent volume claims (default behavior)'
        )

    def handle(self, *args, **options):
        node_name = options['node_name']
        
        try:
            node = Node.objects.get(name=node_name)
        except Node.DoesNotExist:
            raise CommandError(f'Node "{node_name}" does not exist')
        
        if not node.kube_credential:
            raise CommandError(f'Node "{node_name}" has no Kubernetes credentials configured')
        
        if not node.kube_credential.is_active:
            raise CommandError(f'Kubernetes credentials for node "{node_name}" are not active')
        
        # Check current status
        if node.status == 'stopped':
            self.stdout.write(
                self.style.WARNING(f'Node "{node_name}" is already stopped.')
            )
            return
        
        self.stdout.write(f'Stopping node "{node_name}"...')
        
        # Create Kubernetes manager
        k8s_manager = KubernetesNodeManager(node.kube_credential)
        
        # Stop the node
        try:
            with transaction.atomic():
                success = k8s_manager.delete_node(node)
                
                if success:
                    self.stdout.write(
                        self.style.SUCCESS(f'Successfully stopped node "{node_name}"')
                    )
                    self.stdout.write(f'Status: {node.status}')
                    
                    if options['keep_data']:
                        self.stdout.write(
                            self.style.WARNING('Data volumes preserved (use kubectl to delete PVCs if needed)')
                        )
                    
                else:
                    raise CommandError(f'Failed to stop node "{node_name}". Check logs for details.')
                    
        except Exception as e:
            raise CommandError(f'Error stopping node "{node_name}": {str(e)}')