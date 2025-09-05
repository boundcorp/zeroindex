import base64
import logging
from typing import Dict, Any, Optional
from pathlib import Path
from django.template import Template, Context
from django.conf import settings

from .models import Node, KubeCredential

# Optional imports
try:
    import yaml
    import kubernetes
    from kubernetes import client, config
    from kubernetes.client.rest import ApiException
    KUBERNETES_AVAILABLE = True
except ImportError:
    KUBERNETES_AVAILABLE = False
    yaml = None
    kubernetes = None
    client = None
    config = None
    ApiException = Exception

logger = logging.getLogger(__name__)


class KubernetesNodeManager:
    """Service for managing Ethereum node deployments on Kubernetes"""
    
    def __init__(self, kube_credential: KubeCredential):
        if not KUBERNETES_AVAILABLE:
            raise ImportError("Kubernetes Python client not available. Install with: pip install kubernetes pyyaml")
        
        self.kube_credential = kube_credential
        self.namespace = kube_credential.namespace
        self.k8s_client = self._create_k8s_client()
        
    def _create_k8s_client(self):
        """Create Kubernetes API client from stored credentials"""
        try:
            # Decode base64 kubeconfig
            kubeconfig_content = base64.b64decode(self.kube_credential.kubeconfig).decode()
            
            # Write temporary kubeconfig file
            import tempfile
            with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
                f.write(kubeconfig_content)
                kubeconfig_path = f.name
            
            # Load kubeconfig
            config.load_kube_config(config_file=kubeconfig_path)
            
            # Clean up temp file
            Path(kubeconfig_path).unlink()
            
            return client.ApiClient()
        except Exception as e:
            logger.error(f"Failed to create Kubernetes client: {e}")
            raise
    
    def deploy_node(self, node: Node) -> bool:
        """Deploy an Ethereum node with both execution and consensus clients"""
        try:
            logger.info(f"Starting deployment of node {node.name}")
            node.status = 'provisioning'
            node.save()
            
            # Deploy execution client first
            if not self._deploy_execution_client(node):
                node.status = 'error'
                node.save()
                return False
                
            # Deploy consensus client if it's an Ethereum L1 node
            if node.is_ethereum_l1:
                if not self._deploy_consensus_client(node):
                    node.status = 'error'
                    node.save()
                    return False
            
            # Update node status
            node.status = 'syncing'
            node.save()
            logger.info(f"Successfully deployed node {node.name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to deploy node {node.name}: {e}")
            node.status = 'error'
            node.save()
            return False
    
    def _deploy_execution_client(self, node: Node) -> bool:
        """Deploy execution client (e.g., Reth)"""
        try:
            template_path = Path(__file__).parent / "k8s_templates" / f"{node.execution_client}_execution_deployment.yaml"
            
            if not template_path.exists():
                logger.error(f"Template not found: {template_path}")
                return False
            
            # Prepare template context
            context = self._prepare_execution_context(node)
            
            # Render and apply template
            rendered_yaml = self._render_template(template_path, context)
            return self._apply_k8s_resources(rendered_yaml, node)
            
        except Exception as e:
            logger.error(f"Failed to deploy execution client for {node.name}: {e}")
            return False
    
    def _deploy_consensus_client(self, node: Node) -> bool:
        """Deploy consensus client (e.g., Lighthouse)"""
        try:
            template_path = Path(__file__).parent / "k8s_templates" / f"{node.consensus_client}_consensus_deployment.yaml"
            
            if not template_path.exists():
                logger.error(f"Template not found: {template_path}")
                return False
            
            # Prepare template context
            context = self._prepare_consensus_context(node)
            
            # Render and apply template
            rendered_yaml = self._render_template(template_path, context)
            return self._apply_k8s_resources(rendered_yaml, node)
            
        except Exception as e:
            logger.error(f"Failed to deploy consensus client for {node.name}: {e}")
            return False
    
    def _prepare_execution_context(self, node: Node) -> Dict[str, Any]:
        """Prepare template context for execution client"""
        deployment_name = node.get_execution_deployment_name()
        pvc_name = f"{deployment_name}-data"
        
        # Update node with deployment info
        node.execution_deployment_name = deployment_name
        node.execution_pvc_name = pvc_name
        node.save()
        
        return {
            'deployment_name': deployment_name,
            'pvc_name': pvc_name,
            'namespace': self.namespace,
            'chain_name': node.chain.name,
            'node_name': node.name,
            'node_type': node.node_type,
            'version': node.execution_version,
            'storage_size': node.storage_size_gb,
            'storage_class': node.storage_class,
            'resources': node.get_default_execution_resources(),
            'extra_args': node.extra_args,
            'node_selector': node.get_execution_node_selector_dict(),
            'node_affinity': node.get_execution_node_affinity(),
        }
    
    def _prepare_consensus_context(self, node: Node) -> Dict[str, Any]:
        """Prepare template context for consensus client"""
        deployment_name = node.get_consensus_deployment_name()
        pvc_name = f"{deployment_name}-data"
        execution_service = f"{node.get_execution_deployment_name()}-service"
        
        # Update node with deployment info
        node.consensus_deployment_name = deployment_name
        node.consensus_pvc_name = pvc_name
        node.save()
        
        return {
            'deployment_name': deployment_name,
            'pvc_name': pvc_name,
            'namespace': self.namespace,
            'chain_name': node.chain.name,
            'node_name': node.name,
            'node_type': node.node_type,
            'version': node.consensus_version,
            'storage_size': node.consensus_storage_size_gb,
            'storage_class': node.storage_class,
            'resources': node.get_default_consensus_resources(),
            'extra_args': node.extra_args,
            'execution_service': execution_service,
            'fee_recipient': getattr(settings, 'ETHEREUM_FEE_RECIPIENT', None),
            'node_selector': node.get_consensus_node_selector_dict(),
            'node_affinity': node.get_consensus_node_affinity(),
        }
    
    def _render_template(self, template_path: Path, context: Dict[str, Any]) -> str:
        """Render Kubernetes YAML template with Django template engine"""
        with open(template_path, 'r') as f:
            template_content = f.read()
        
        template = Template(template_content)
        django_context = Context(context)
        
        return template.render(django_context)
    
    def _apply_k8s_resources(self, yaml_content: str, node: Node) -> bool:
        """Apply Kubernetes resources from YAML"""
        try:
            # Parse YAML documents
            resources = list(yaml.safe_load_all(yaml_content))
            
            for resource in resources:
                if not resource:
                    continue
                    
                self._apply_single_resource(resource)
                
            return True
            
        except Exception as e:
            logger.error(f"Failed to apply K8s resources for {node.name}: {e}")
            return False
    
    def _apply_single_resource(self, resource: Dict[str, Any]):
        """Apply a single Kubernetes resource"""
        kind = resource.get('kind')
        api_version = resource.get('apiVersion')
        
        if kind == 'Deployment':
            apps_v1 = client.AppsV1Api(self.k8s_client)
            try:
                apps_v1.create_namespaced_deployment(
                    namespace=self.namespace,
                    body=resource
                )
            except ApiException as e:
                if e.status == 409:  # Already exists
                    apps_v1.patch_namespaced_deployment(
                        name=resource['metadata']['name'],
                        namespace=self.namespace,
                        body=resource
                    )
                else:
                    raise
                    
        elif kind == 'Service':
            core_v1 = client.CoreV1Api(self.k8s_client)
            try:
                core_v1.create_namespaced_service(
                    namespace=self.namespace,
                    body=resource
                )
            except ApiException as e:
                if e.status == 409:  # Already exists
                    core_v1.patch_namespaced_service(
                        name=resource['metadata']['name'],
                        namespace=self.namespace,
                        body=resource
                    )
                else:
                    raise
                    
        elif kind == 'PersistentVolumeClaim':
            core_v1 = client.CoreV1Api(self.k8s_client)
            try:
                core_v1.create_namespaced_persistent_volume_claim(
                    namespace=self.namespace,
                    body=resource
                )
            except ApiException as e:
                if e.status == 409:  # Already exists, skip
                    logger.info(f"PVC {resource['metadata']['name']} already exists")
                else:
                    raise
                    
        elif kind == 'Job':
            batch_v1 = client.BatchV1Api(self.k8s_client)
            try:
                batch_v1.create_namespaced_job(
                    namespace=self.namespace,
                    body=resource
                )
            except ApiException as e:
                if e.status == 409:  # Already exists, skip
                    logger.info(f"Job {resource['metadata']['name']} already exists")
                else:
                    raise
    
    def delete_node(self, node: Node) -> bool:
        """Delete node deployments from Kubernetes"""
        try:
            logger.info(f"Deleting deployments for node {node.name}")
            
            # Delete execution client resources
            if node.execution_deployment_name:
                self._delete_deployment_resources(node.execution_deployment_name)
            
            # Delete consensus client resources if they exist
            if node.consensus_deployment_name:
                self._delete_deployment_resources(node.consensus_deployment_name)
            
            node.status = 'stopped'
            node.save()
            logger.info(f"Successfully deleted node {node.name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete node {node.name}: {e}")
            return False
    
    def _delete_deployment_resources(self, deployment_name: str):
        """Delete all resources for a deployment"""
        apps_v1 = client.AppsV1Api(self.k8s_client)
        core_v1 = client.CoreV1Api(self.k8s_client)
        batch_v1 = client.BatchV1Api(self.k8s_client)
        
        try:
            # Delete deployment
            apps_v1.delete_namespaced_deployment(
                name=deployment_name,
                namespace=self.namespace
            )
        except ApiException as e:
            if e.status != 404:
                raise
        
        try:
            # Delete service
            core_v1.delete_namespaced_service(
                name=f"{deployment_name}-service",
                namespace=self.namespace
            )
        except ApiException as e:
            if e.status != 404:
                raise
        
        # Note: We don't delete PVCs to preserve data
        logger.info(f"Deleted resources for deployment {deployment_name}")
    
    def get_node_status(self, node: Node) -> Dict[str, Any]:
        """Get current status of node deployments"""
        status = {
            'execution_client': self._get_deployment_status(node.execution_deployment_name),
            'consensus_client': self._get_deployment_status(node.consensus_deployment_name) if node.is_ethereum_l1 else None,
            'storage': self._get_pvc_usage_status(node),
        }
        
        return status
    
    def get_storage_status(self, node: Node) -> Dict[str, Any]:
        """Get storage status for a node - alias for _get_pvc_usage_status"""
        return self._get_pvc_usage_status(node)
    
    def _get_deployment_status(self, deployment_name: str) -> Optional[Dict[str, Any]]:
        """Get status of a specific deployment"""
        if not deployment_name:
            return None
            
        try:
            apps_v1 = client.AppsV1Api(self.k8s_client)
            deployment = apps_v1.read_namespaced_deployment(
                name=deployment_name,
                namespace=self.namespace
            )
            
            return {
                'ready_replicas': deployment.status.ready_replicas or 0,
                'replicas': deployment.status.replicas or 0,
                'available_replicas': deployment.status.available_replicas or 0,
                'conditions': [
                    {
                        'type': condition.type,
                        'status': condition.status,
                        'reason': condition.reason,
                        'message': condition.message,
                        'last_transition_time': condition.last_transition_time
                    }
                    for condition in (deployment.status.conditions or [])
                ]
            }
        except ApiException as e:
            if e.status == 404:
                return {'error': 'Deployment not found'}
            raise
    
    def _get_pvc_usage_status(self, node: Node) -> Dict[str, Any]:
        """Get PVC disk usage status for a node"""
        storage_status = {}
        
        # Get execution client PVC status
        if node.execution_pvc_name:
            exec_usage = self._get_single_pvc_usage(node.execution_pvc_name)
            if exec_usage:
                storage_status['execution'] = exec_usage
        
        # Get consensus client PVC status (if L1 Ethereum)
        if node.is_ethereum_l1 and node.consensus_pvc_name:
            cons_usage = self._get_single_pvc_usage(node.consensus_pvc_name)
            if cons_usage:
                storage_status['consensus'] = cons_usage
        
        # Get shared JWT PVC status
        jwt_pvc_name = f"{node.name}-jwt-shared"
        jwt_usage = self._get_single_pvc_usage(jwt_pvc_name)
        if jwt_usage:
            storage_status['jwt_shared'] = jwt_usage
        
        return storage_status
    
    def _get_single_pvc_usage(self, pvc_name: str) -> Optional[Dict[str, Any]]:
        """Get disk usage for a single PVC"""
        try:
            core_v1 = client.CoreV1Api(self.k8s_client)
            
            # Get PVC info
            pvc = core_v1.read_namespaced_persistent_volume_claim(
                name=pvc_name,
                namespace=self.namespace
            )
            
            # Get capacity from PVC spec
            capacity_str = pvc.spec.resources.requests.get('storage', '0Gi')
            capacity_bytes = self._parse_storage_size(capacity_str)
            
            # Try to get actual usage by checking if there's a pod using this PVC
            usage_bytes = self._get_pvc_actual_usage(pvc_name)
            
            usage_percentage = (usage_bytes / capacity_bytes * 100) if capacity_bytes > 0 else 0
            
            available_bytes = capacity_bytes - usage_bytes
            
            return {
                'pvc_name': pvc_name,
                'namespace': self.namespace,
                'storage_class': pvc.spec.storage_class_name or 'Unknown',
                'capacity': capacity_str,
                'capacity_bytes': capacity_bytes,
                'used_bytes': usage_bytes,
                'available_bytes': available_bytes,
                'usage_percentage': round(usage_percentage, 1),
                'status': pvc.status.phase if pvc.status else 'Unknown'
            }
            
        except ApiException as e:
            if e.status == 404:
                return {'error': f'PVC {pvc_name} not found'}
            return {'error': f'Failed to get PVC status: {str(e)}'}
        except Exception as e:
            return {'error': f'Error getting PVC usage: {str(e)}'}
    
    def _get_pvc_actual_usage(self, pvc_name: str) -> int:
        """Get actual disk usage for a PVC by checking pod filesystem usage"""
        try:
            core_v1 = client.CoreV1Api(self.k8s_client)
            
            # Find pods using this PVC
            pods = core_v1.list_namespaced_pod(namespace=self.namespace)
            
            for pod in pods.items:
                if not pod.spec.volumes:
                    continue
                    
                # Check if this pod uses our PVC
                uses_pvc = False
                mount_path = None
                
                for volume in pod.spec.volumes:
                    if (volume.persistent_volume_claim and 
                        volume.persistent_volume_claim.claim_name == pvc_name):
                        uses_pvc = True
                        # Find mount path from container volume mounts
                        if pod.spec.containers:
                            for container in pod.spec.containers:
                                if container.volume_mounts:
                                    for mount in container.volume_mounts:
                                        if mount.name == volume.name:
                                            mount_path = mount.mount_path
                                            break
                        break
                
                if uses_pvc and pod.status.phase == 'Running' and mount_path:
                    # Try to get disk usage from the pod
                    return self._exec_df_command(pod.metadata.name, mount_path)
            
            return 0  # No running pods or unable to determine usage
            
        except Exception:
            return 0  # Fallback to 0 if we can't determine usage
    
    def _exec_df_command(self, pod_name: str, mount_path: str) -> int:
        """Execute du command in pod to get disk usage"""
        try:
            from kubernetes.stream import stream
            
            core_v1 = client.CoreV1Api(self.k8s_client)
            
            # Use du command to get directory size
            command = ['sh', '-c', f'du -sb {mount_path} 2>/dev/null | cut -f1 || echo 0']
            
            response = stream(
                core_v1.connect_get_namespaced_pod_exec,
                name=pod_name,
                namespace=self.namespace,
                command=command,
                stderr=True, 
                stdin=False,
                stdout=True, 
                tty=False
            )
            
            # Parse output to get used bytes
            output = response.strip()
            
            if output and output.isdigit():
                return int(output)
            
            return 0
            
        except Exception as e:
            logger.debug(f"Failed to get disk usage for {pod_name}:{mount_path}: {e}")
            return 0  # Fallback if command execution fails
    
    def _parse_storage_size(self, size_str: str) -> int:
        """Parse Kubernetes storage size string to bytes"""
        size_str = size_str.strip().upper()
        
        if size_str.endswith('TI'):
            return int(float(size_str[:-2]) * 1024 * 1024 * 1024 * 1024)
        elif size_str.endswith('T'):
            return int(float(size_str[:-1]) * 1000 * 1000 * 1000 * 1000)
        elif size_str.endswith('GI'):
            return int(float(size_str[:-2]) * 1024 * 1024 * 1024)
        elif size_str.endswith('G'):
            return int(float(size_str[:-1]) * 1000 * 1000 * 1000)
        elif size_str.endswith('MI'):
            return int(float(size_str[:-2]) * 1024 * 1024)
        elif size_str.endswith('M'):
            return int(float(size_str[:-1]) * 1000 * 1000)
        elif size_str.endswith('KI'):
            return int(float(size_str[:-2]) * 1024)
        elif size_str.endswith('K'):
            return int(float(size_str[:-1]) * 1000)
        else:
            # Assume bytes
            return int(float(size_str))