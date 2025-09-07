"""
Management command to set up HOME cluster credentials for blockchain node deployment.
"""
import base64
import os
from django.core.management.base import BaseCommand, CommandError
from zeroindex.apps.nodes.models import KubeCredential
from zeroindex.apps.chains.models import Chain


class Command(BaseCommand):
    help = 'Set up HOME cluster Kubernetes credentials for blockchain deployments'

    def add_arguments(self, parser):
        parser.add_argument(
            '--kubeconfig-path',
            type=str,
            default=os.path.expanduser('~/.kube/clusters/home'),
            help='Path to HOME cluster kubeconfig file'
        )
        parser.add_argument(
            '--namespace',
            type=str,
            default='devbox',
            help='Kubernetes namespace for deployments (default: devbox)'
        )
        parser.add_argument(
            '--name',
            type=str,
            default='home-cluster',
            help='Name for the credential entry'
        )

    def handle(self, *args, **options):
        kubeconfig_path = options['kubeconfig_path']
        namespace = options['namespace']
        name = options['name']
        
        # Check if kubeconfig file exists
        if not os.path.exists(kubeconfig_path):
            raise CommandError(f'Kubeconfig file not found: {kubeconfig_path}')
        
        # Read and encode kubeconfig
        with open(kubeconfig_path, 'r') as f:
            kubeconfig_content = f.read()
        
        kubeconfig_b64 = base64.b64encode(kubeconfig_content.encode()).decode()
        
        # Check if credential already exists
        existing = KubeCredential.objects.filter(name=name).first()
        if existing:
            self.stdout.write(f'Updating existing credential: {name}')
            existing.namespace = namespace
            existing.kubeconfig = kubeconfig_b64
            existing.is_active = True
            existing.save()
            credential = existing
        else:
            # Create new credential
            credential = KubeCredential.objects.create(
                name=name,
                cluster_name='HOME',
                namespace=namespace,
                kubeconfig=kubeconfig_b64,
                is_active=True
            )
            self.stdout.write(self.style.SUCCESS(f'Created new credential: {name}'))
        
        # Ensure Ethereum mainnet chain exists
        chain, created = Chain.objects.get_or_create(
            chain_id=1,
            defaults={
                'name': 'Ethereum',
                'symbol': 'ETH',
                'is_testnet': False
            }
        )
        if created:
            self.stdout.write(self.style.SUCCESS('Created Ethereum mainnet chain'))
        
        # Display summary
        self.stdout.write(f'\n{self.style.HTTP_INFO("Configuration Summary:")}')
        self.stdout.write(f'  Credential: {credential.name}')
        self.stdout.write(f'  Cluster: {credential.cluster_name}')
        self.stdout.write(f'  Namespace: {credential.namespace}')
        self.stdout.write(f'  Active: {credential.is_active}')
        self.stdout.write(f'  Chain: {chain.name} (ID: {chain.chain_id})')
        
        self.stdout.write(f'\n{self.style.SUCCESS("✓ HOME cluster is ready for blockchain deployments")}')
        self.stdout.write('\nTo create a node, run:')
        self.stdout.write('  python manage.py create_node eth-mainnet-01 --start')
        self.stdout.write('\nRecommended node configuration:')
        self.stdout.write('  - Deploy to vega or nova nodes (avoid enterprise/ziti)')
        self.stdout.write('  - Use nfs-iota-hdd-slush storage class')
        self.stdout.write('  - Consensus needs 12Gi memory limit minimum')
        self.stdout.write('  - Execution needs 16Gi memory limit for full node')