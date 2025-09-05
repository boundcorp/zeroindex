"""
Management command to update node resource limits in Kubernetes deployments.
"""
import json
import subprocess
from django.core.management.base import BaseCommand, CommandError
from zeroindex.apps.nodes.models import Node
from zeroindex.apps.chains.models import Chain


class Command(BaseCommand):
    help = 'Update resource limits for node deployments'

    def add_arguments(self, parser):
        parser.add_argument(
            '--node-name',
            type=str,
            required=True,
            help='Name of the node to update'
        )
        parser.add_argument(
            '--component',
            type=str,
            choices=['execution', 'consensus', 'both'],
            default='both',
            help='Which component to update'
        )
        parser.add_argument(
            '--memory-limit',
            type=str,
            help='Memory limit (e.g., 12Gi)'
        )
        parser.add_argument(
            '--cpu-limit',
            type=str,
            help='CPU limit (e.g., 4)'
        )
        parser.add_argument(
            '--memory-request',
            type=str,
            help='Memory request (e.g., 8Gi)'
        )
        parser.add_argument(
            '--cpu-request',
            type=str,
            help='CPU request (e.g., 2)'
        )
        parser.add_argument(
            '--liveness-timeout',
            type=int,
            help='Liveness probe timeout in seconds'
        )
        parser.add_argument(
            '--liveness-period',
            type=int,
            help='Liveness probe period in seconds'
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be updated without applying changes'
        )

    def handle(self, *args, **options):
        node_name = options['node_name']
        component = options['component']
        dry_run = options['dry_run']
        
        # Build patch for resources
        patches = []
        
        if component in ['execution', 'both']:
            exec_patch = self.build_resource_patch(
                deployment_name=f"{node_name}-execution",
                memory_limit=options.get('memory_limit'),
                cpu_limit=options.get('cpu_limit'),
                memory_request=options.get('memory_request'),
                cpu_request=options.get('cpu_request'),
                liveness_timeout=options.get('liveness_timeout'),
                liveness_period=options.get('liveness_period')
            )
            if exec_patch:
                patches.append(('execution', f"{node_name}-execution", exec_patch))
        
        if component in ['consensus', 'both']:
            consensus_patch = self.build_resource_patch(
                deployment_name=f"{node_name}-consensus",
                memory_limit=options.get('memory_limit'),
                cpu_limit=options.get('cpu_limit'),
                memory_request=options.get('memory_request'),
                cpu_request=options.get('cpu_request'),
                liveness_timeout=options.get('liveness_timeout'),
                liveness_period=options.get('liveness_period')
            )
            if consensus_patch:
                patches.append(('consensus', f"{node_name}-consensus", consensus_patch))
        
        if not patches:
            self.stdout.write(self.style.WARNING('No updates specified'))
            return
        
        # Apply patches
        for component_type, deployment_name, patch in patches:
            self.stdout.write(f"\nUpdating {component_type} deployment: {deployment_name}")
            
            if dry_run:
                self.stdout.write(self.style.NOTICE('DRY RUN - Would apply patch:'))
                self.stdout.write(json.dumps(patch, indent=2))
            else:
                try:
                    # Apply the patch
                    result = self.apply_patch(deployment_name, patch)
                    if result:
                        self.stdout.write(self.style.SUCCESS(f'✓ Updated {deployment_name}'))
                    else:
                        self.stdout.write(self.style.ERROR(f'✗ Failed to update {deployment_name}'))
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f'Error updating {deployment_name}: {e}'))
    
    def build_resource_patch(self, deployment_name, memory_limit=None, cpu_limit=None,
                            memory_request=None, cpu_request=None,
                            liveness_timeout=None, liveness_period=None):
        """Build a JSON patch for the deployment"""
        patch = {}
        
        # Build resource patch
        if any([memory_limit, cpu_limit, memory_request, cpu_request]):
            containers_patch = []
            container_patch = {}
            
            if memory_limit or cpu_limit:
                container_patch['resources'] = container_patch.get('resources', {})
                container_patch['resources']['limits'] = {}
                if memory_limit:
                    container_patch['resources']['limits']['memory'] = memory_limit
                if cpu_limit:
                    container_patch['resources']['limits']['cpu'] = cpu_limit
            
            if memory_request or cpu_request:
                container_patch['resources'] = container_patch.get('resources', {})
                container_patch['resources']['requests'] = {}
                if memory_request:
                    container_patch['resources']['requests']['memory'] = memory_request
                if cpu_request:
                    container_patch['resources']['requests']['cpu'] = cpu_request
            
            # Add liveness probe updates
            if liveness_timeout or liveness_period:
                container_patch['livenessProbe'] = {}
                if liveness_timeout:
                    container_patch['livenessProbe']['timeoutSeconds'] = liveness_timeout
                if liveness_period:
                    container_patch['livenessProbe']['periodSeconds'] = liveness_period
            
            if container_patch:
                patch['spec'] = {
                    'template': {
                        'spec': {
                            'containers': [container_patch]
                        }
                    }
                }
        
        return patch if patch else None
    
    def apply_patch(self, deployment_name, patch):
        """Apply a patch to a deployment using kubectl"""
        namespace = 'devbox'
        
        # First, get the current deployment to merge patches properly
        get_cmd = [
            'kubectl', 'get', 'deployment', deployment_name,
            '-n', namespace, '-o', 'json'
        ]
        
        try:
            result = subprocess.run(get_cmd, capture_output=True, text=True, check=True)
            current_deployment = json.loads(result.stdout)
            
            # Merge the patch with current container settings
            if 'spec' in patch and 'template' in patch['spec']:
                containers = current_deployment['spec']['template']['spec']['containers']
                patch_container = patch['spec']['template']['spec']['containers'][0]
                
                # Find the main container (not init containers)
                for i, container in enumerate(containers):
                    if container['name'] in ['geth', 'lighthouse-beacon']:
                        # Merge resources
                        if 'resources' in patch_container:
                            if 'resources' not in containers[i]:
                                containers[i]['resources'] = {}
                            if 'limits' in patch_container['resources']:
                                if 'limits' not in containers[i]['resources']:
                                    containers[i]['resources']['limits'] = {}
                                containers[i]['resources']['limits'].update(patch_container['resources']['limits'])
                            if 'requests' in patch_container['resources']:
                                if 'requests' not in containers[i]['resources']:
                                    containers[i]['resources']['requests'] = {}
                                containers[i]['resources']['requests'].update(patch_container['resources']['requests'])
                        
                        # Merge liveness probe
                        if 'livenessProbe' in patch_container:
                            if 'livenessProbe' not in containers[i]:
                                containers[i]['livenessProbe'] = {}
                            containers[i]['livenessProbe'].update(patch_container['livenessProbe'])
                        break
            
            # Apply the updated deployment
            apply_cmd = [
                'kubectl', 'apply', '-n', namespace, '-f', '-'
            ]
            
            result = subprocess.run(
                apply_cmd,
                input=json.dumps(current_deployment),
                capture_output=True,
                text=True,
                check=True
            )
            
            return True
            
        except subprocess.CalledProcessError as e:
            self.stdout.write(self.style.ERROR(f'kubectl error: {e.stderr}'))
            return False
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Error: {e}'))
            return False