from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from zeroindex.apps.nodes.models import Node, KubeCredential
from zeroindex.apps.chains.models import Chain


class Command(BaseCommand):
    help = 'Create a new Ethereum node'

    def add_arguments(self, parser):
        parser.add_argument(
            'name',
            type=str,
            help='Name for the node'
        )
        parser.add_argument(
            '--chain-id',
            type=int,
            default=1,
            help='Chain ID (default: 1 for Ethereum mainnet)'
        )
        parser.add_argument(
            '--node-type',
            choices=['full', 'archive', 'light', 'validator'],
            default='full',
            help='Type of node (default: full)'
        )
        parser.add_argument(
            '--execution-client',
            choices=['reth', 'geth', 'erigon', 'besu', 'nethermind'],
            default='reth',
            help='Execution client (default: reth)'
        )
        parser.add_argument(
            '--consensus-client',
            choices=['lighthouse', 'prysm', 'teku', 'nimbus', 'lodestar'],
            default='lighthouse',
            help='Consensus client for Ethereum L1 (default: lighthouse)'
        )
        parser.add_argument(
            '--execution-version',
            type=str,
            default='latest',
            help='Execution client version (default: latest)'
        )
        parser.add_argument(
            '--consensus-version',
            type=str,
            default='latest',
            help='Consensus client version (default: latest)'
        )
        parser.add_argument(
            '--storage-size',
            type=int,
            default=2000,
            help='Storage size in GB for execution client (default: 2000)'
        )
        parser.add_argument(
            '--consensus-storage-size',
            type=int,
            default=200,
            help='Storage size in GB for consensus client (default: 200)'
        )
        parser.add_argument(
            '--kube-credential',
            type=str,
            help='Name of Kubernetes credential to use (defaults to first active one)'
        )
        parser.add_argument(
            '--start',
            action='store_true',
            help='Start the node immediately after creation'
        )

    def handle(self, *args, **options):
        name = options['name']
        
        # Check if node with this name already exists
        if Node.objects.filter(name=name).exists():
            raise CommandError(f'Node with name "{name}" already exists')
        
        # Get chain
        try:
            chain = Chain.objects.get(chain_id=options['chain_id'])
        except Chain.DoesNotExist:
            raise CommandError(f'Chain with ID {options["chain_id"]} does not exist')
        
        # Get Kubernetes credential
        if options['kube_credential']:
            try:
                kube_cred = KubeCredential.objects.get(
                    name=options['kube_credential'],
                    is_active=True
                )
            except KubeCredential.DoesNotExist:
                raise CommandError(f'Active Kubernetes credential "{options["kube_credential"]}" not found')
        else:
            kube_cred = KubeCredential.objects.filter(is_active=True).first()
            if not kube_cred:
                raise CommandError('No active Kubernetes credentials found')
        
        self.stdout.write(f'Creating node "{name}"...')
        self.stdout.write(f'Chain: {chain.name} (ID: {chain.chain_id})')
        self.stdout.write(f'Type: {options["node_type"]}')
        self.stdout.write(f'Execution client: {options["execution_client"]} {options["execution_version"]}')
        
        is_ethereum_l1 = chain.chain_id == 1
        if is_ethereum_l1:
            self.stdout.write(f'Consensus client: {options["consensus_client"]} {options["consensus_version"]}')
        
        self.stdout.write(f'Kubernetes: {kube_cred.name} ({kube_cred.cluster_name}/{kube_cred.namespace})')
        
        # Create the node
        with transaction.atomic():
            node = Node.objects.create(
                name=name,
                chain=chain,
                node_type=options['node_type'],
                execution_client=options['execution_client'],
                execution_version=options['execution_version'],
                consensus_client=options['consensus_client'] if is_ethereum_l1 else 'lighthouse',
                consensus_version=options['consensus_version'] if is_ethereum_l1 else 'latest',
                storage_size_gb=options['storage_size'],
                consensus_storage_size_gb=options['consensus_storage_size'],
                kube_credential=kube_cred,
                status='pending'
            )
            
            # Set default resource configurations
            exec_resources = node.get_default_execution_resources()
            cons_resources = node.get_default_consensus_resources()
            
            node.resource_requests = exec_resources['requests']
            node.resource_limits = exec_resources['limits']
            node.save()
            
            self.stdout.write(
                self.style.SUCCESS(f'Successfully created node "{name}" (ID: {node.id})')
            )
            
            # Show configuration summary
            self.stdout.write(f'\n{self.style.HTTP_INFO("Configuration:")}')
            self.stdout.write(f'  Storage (execution): {node.storage_size_gb} GB')
            if is_ethereum_l1:
                self.stdout.write(f'  Storage (consensus): {node.consensus_storage_size_gb} GB')
            
            self.stdout.write(f'  Resources (execution):')
            self.stdout.write(f'    Requests: {exec_resources["requests"]}')
            self.stdout.write(f'    Limits: {exec_resources["limits"]}')
            
            if is_ethereum_l1:
                self.stdout.write(f'  Resources (consensus):')
                self.stdout.write(f'    Requests: {cons_resources["requests"]}')
                self.stdout.write(f'    Limits: {cons_resources["limits"]}')
            
            # Start node if requested
            if options['start']:
                self.stdout.write(f'\nStarting node "{name}"...')
                from zeroindex.apps.nodes.services import KubernetesNodeManager
                
                k8s_manager = KubernetesNodeManager(kube_cred)
                success = k8s_manager.deploy_node(node)
                
                if success:
                    self.stdout.write(
                        self.style.SUCCESS(f'Node "{name}" started successfully')
                    )
                    self.stdout.write(f'Status: {node.status}')
                else:
                    self.stdout.write(
                        self.style.ERROR(f'Failed to start node "{name}". Use "python manage.py start_node {name}" to retry.')
                    )
            else:
                self.stdout.write(f'\nUse "python manage.py start_node {name}" to start the node.')
        
        self.stdout.write(f'Use "python manage.py node_status {name}" to check status.')