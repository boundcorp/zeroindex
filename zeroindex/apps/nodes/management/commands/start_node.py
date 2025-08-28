from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from zeroindex.apps.nodes.models import Node
from zeroindex.apps.nodes.services import KubernetesNodeManager


class Command(BaseCommand):
    help = 'Start an Ethereum node deployment'

    def add_arguments(self, parser):
        parser.add_argument(
            'node_name',
            type=str,
            help='Name of the node to start'
        )
        parser.add_argument(
            '--force',
            action='store_true',
            help='Force restart even if already running'
        )

    def handle(self, *args, **options):
        node_name = options['node_name']
        force = options['force']
        
        try:
            node = Node.objects.get(name=node_name)
        except Node.DoesNotExist:
            raise CommandError(f'Node "{node_name}" does not exist')
        
        if not node.kube_credential:
            raise CommandError(f'Node "{node_name}" has no Kubernetes credentials configured')
        
        if not node.kube_credential.is_active:
            raise CommandError(f'Kubernetes credentials for node "{node_name}" are not active')
        
        # Check current status
        if node.status in ['running', 'syncing', 'provisioning'] and not force:
            self.stdout.write(
                self.style.WARNING(f'Node "{node_name}" is already {node.status}. Use --force to restart.')
            )
            return
        
        self.stdout.write(f'Starting node "{node_name}"...')
        
        # Create Kubernetes manager
        k8s_manager = KubernetesNodeManager(node.kube_credential)
        
        # Deploy the node
        try:
            with transaction.atomic():
                success = k8s_manager.deploy_node(node)
                
                if success:
                    self.stdout.write(
                        self.style.SUCCESS(f'Successfully started node "{node_name}"')
                    )
                    self.stdout.write(f'Status: {node.status}')
                    
                    if node.is_ethereum_l1:
                        self.stdout.write(f'Execution client: {node.execution_client}')
                        self.stdout.write(f'Consensus client: {node.consensus_client}')
                        self.stdout.write(f'Execution deployment: {node.execution_deployment_name}')
                        self.stdout.write(f'Consensus deployment: {node.consensus_deployment_name}')
                    else:
                        self.stdout.write(f'Client: {node.execution_client}')
                        self.stdout.write(f'Deployment: {node.execution_deployment_name}')
                    
                    self.stdout.write(f'Namespace: {node.kube_credential.namespace}')
                    
                else:
                    raise CommandError(f'Failed to start node "{node_name}". Check logs for details.')
                    
        except Exception as e:
            raise CommandError(f'Error starting node "{node_name}": {str(e)}')