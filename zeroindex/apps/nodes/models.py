from django.db import models
from django.contrib.postgres.fields import JSONField
from django.utils import timezone
from datetime import timedelta
from zeroindex.apps.chains.models import Chain


class KubeCredential(models.Model):
    name = models.CharField(max_length=255, unique=True)
    cluster_name = models.CharField(max_length=255)
    namespace = models.CharField(max_length=255, default='default')
    kubeconfig = models.TextField(
        help_text="Base64 encoded kubeconfig or path to kubeconfig file"
    )
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = 'Kubernetes Credential'
        verbose_name_plural = 'Kubernetes Credentials'

    def __str__(self):
        return f"{self.name} - {self.cluster_name}/{self.namespace}"


class Node(models.Model):
    NODE_TYPE_CHOICES = [
        ('archive', 'Archive Node'),
        ('full', 'Full Node'),
        ('light', 'Light Node'),
        ('validator', 'Validator Node'),
    ]
    
    STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('provisioning', 'Provisioning'),
        ('syncing', 'Syncing'),
        ('running', 'Running'),
        ('error', 'Error'),
        ('stopped', 'Stopped'),
    ]

    EXECUTION_CLIENT_CHOICES = [
        ('reth', 'Reth'),
        ('geth', 'Geth'),
        ('erigon', 'Erigon'),
        ('besu', 'Besu'),
        ('nethermind', 'Nethermind'),
    ]

    CONSENSUS_CLIENT_CHOICES = [
        ('lighthouse', 'Lighthouse'),
        ('prysm', 'Prysm'),
        ('teku', 'Teku'),
        ('nimbus', 'Nimbus'),
        ('lodestar', 'Lodestar'),
    ]

    name = models.CharField(max_length=255, unique=True)
    chain = models.ForeignKey(
        Chain,
        on_delete=models.CASCADE,
        related_name='nodes'
    )
    node_type = models.CharField(max_length=20, choices=NODE_TYPE_CHOICES, default='full')
    
    # For Ethereum L1 nodes, we need both execution and consensus clients
    execution_client = models.CharField(
        max_length=20, 
        choices=EXECUTION_CLIENT_CHOICES, 
        default='reth',
        help_text="Execution layer client (e.g., reth, geth)"
    )
    execution_version = models.CharField(max_length=50, blank=True)
    
    consensus_client = models.CharField(
        max_length=20, 
        choices=CONSENSUS_CLIENT_CHOICES, 
        default='lighthouse',
        help_text="Consensus layer client (e.g., lighthouse, prysm)"
    )
    consensus_version = models.CharField(max_length=50, blank=True)
    
    # Deprecated - keeping for backward compatibility
    client_software = models.CharField(
        max_length=100,
        blank=True,
        help_text="Deprecated: Use execution_client instead"
    )
    version = models.CharField(max_length=50, blank=True, help_text="Deprecated: Use execution_version instead")
    
    kube_credential = models.ForeignKey(
        KubeCredential,
        on_delete=models.SET_NULL,
        null=True,
        blank=True
    )
    # Kubernetes deployment configuration
    execution_deployment_name = models.CharField(max_length=255, blank=True)
    consensus_deployment_name = models.CharField(max_length=255, blank=True)
    execution_pvc_name = models.CharField(max_length=255, blank=True)
    consensus_pvc_name = models.CharField(max_length=255, blank=True)
    storage_size_gb = models.IntegerField(default=2000, help_text="Storage size for execution client data")
    consensus_storage_size_gb = models.IntegerField(default=200, help_text="Storage size for consensus client data")
    storage_class = models.CharField(
        max_length=100, 
        blank=True, 
        default="iota-slush",
        help_text="Kubernetes storage class for PVCs (e.g., iota-slush)"
    )
    
    # Deprecated fields for backward compatibility
    deployment_name = models.CharField(max_length=255, blank=True, help_text="Deprecated: Use execution_deployment_name")
    pvc_name = models.CharField(max_length=255, blank=True, help_text="Deprecated: Use execution_pvc_name")
    
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending')
    
    # Network endpoints
    execution_rpc_url = models.URLField(blank=True, null=True)
    execution_ws_url = models.URLField(blank=True, null=True)
    consensus_api_url = models.URLField(blank=True, null=True)
    
    # Deprecated endpoints for backward compatibility
    internal_rpc_url = models.URLField(blank=True, null=True, help_text="Deprecated: Use execution_rpc_url")
    external_rpc_url = models.URLField(blank=True, null=True, help_text="Deprecated: Use execution_rpc_url")
    ws_url = models.URLField(blank=True, null=True, help_text="Deprecated: Use execution_ws_url")
    
    # Sync status tracking
    execution_sync_progress = models.FloatField(
        default=0.0,
        help_text="Execution client sync progress as percentage (0-100)"
    )
    consensus_sync_progress = models.FloatField(
        default=0.0,
        help_text="Consensus client sync progress as percentage (0-100)"
    )
    current_block_height = models.BigIntegerField(null=True, blank=True)
    consensus_head_slot = models.BigIntegerField(null=True, blank=True)
    
    # Deprecated sync field for backward compatibility
    sync_progress = models.FloatField(
        default=0.0,
        help_text="Deprecated: Use execution_sync_progress and consensus_sync_progress"
    )
    
    resource_requests = models.JSONField(
        default=dict,
        blank=True,
        help_text="CPU and memory requests for k8s deployment"
    )
    resource_limits = models.JSONField(
        default=dict,
        blank=True,
        help_text="CPU and memory limits for k8s deployment"
    )
    
    extra_args = models.JSONField(
        default=list,
        blank=True,
        help_text="Additional command line arguments for the node"
    )
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    last_health_check = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ['-created_at']
        verbose_name = 'Blockchain Node'
        verbose_name_plural = 'Blockchain Nodes'
        indexes = [
            models.Index(fields=['chain', 'status']),
            models.Index(fields=['kube_credential', 'status']),
        ]

    def __str__(self):
        return f"{self.name} - {self.chain.name} ({self.execution_client}/{self.consensus_client}) [{self.status}]"

    @property
    def is_ethereum_l1(self):
        """Check if this is an Ethereum L1 node (chain_id = 1)"""
        return self.chain.chain_id == 1

    @property
    def overall_sync_progress(self):
        """Calculate overall sync progress from both execution and consensus clients"""
        if self.is_ethereum_l1:
            # For Ethereum L1, both clients need to be synced
            return min(self.execution_sync_progress, self.consensus_sync_progress)
        else:
            # For other chains, just use execution client progress
            return self.execution_sync_progress

    @property
    def is_fully_synced(self):
        """Check if both execution and consensus clients are fully synced"""
        if self.is_ethereum_l1:
            return self.execution_sync_progress >= 99.9 and self.consensus_sync_progress >= 99.9
        else:
            return self.execution_sync_progress >= 99.9

    def get_execution_deployment_name(self):
        """Get the Kubernetes deployment name for execution client"""
        return self.execution_deployment_name or f"{self.name}-execution"

    def get_consensus_deployment_name(self):
        """Get the Kubernetes deployment name for consensus client"""
        return self.consensus_deployment_name or f"{self.name}-consensus"

    def get_default_execution_resources(self):
        """Get default resource requirements for execution client"""
        if self.node_type == 'archive':
            return {
                'requests': {'cpu': '4', 'memory': '16Gi'},
                'limits': {'cpu': '8', 'memory': '32Gi'}
            }
        elif self.node_type == 'full':
            return {
                'requests': {'cpu': '2', 'memory': '8Gi'},
                'limits': {'cpu': '4', 'memory': '16Gi'}
            }
        else:
            return {
                'requests': {'cpu': '1', 'memory': '4Gi'},
                'limits': {'cpu': '2', 'memory': '8Gi'}
            }

    def get_default_consensus_resources(self):
        """Get default resource requirements for consensus client"""
        if self.node_type == 'validator':
            return {
                'requests': {'cpu': '2', 'memory': '6Gi'},
                'limits': {'cpu': '4', 'memory': '12Gi'}
            }
        else:
            return {
                'requests': {'cpu': '1', 'memory': '4Gi'},
                'limits': {'cpu': '2', 'memory': '8Gi'}
            }

    def get_default_resource_requests(self):
        """Deprecated: Use get_default_execution_resources instead"""
        return self.get_default_execution_resources()['requests']

    def get_default_resource_limits(self):
        """Deprecated: Use get_default_execution_resources instead"""
        return self.get_default_execution_resources()['limits']


class SyncStatusHistory(models.Model):
    """Historical record of node sync status"""
    node = models.ForeignKey(
        Node,
        on_delete=models.CASCADE,
        related_name='sync_history'
    )
    timestamp = models.DateTimeField(auto_now_add=True, db_index=True)
    
    # Sync progress
    execution_sync_progress = models.FloatField(
        default=0.0,
        help_text="Execution client sync progress (0-100)"
    )
    consensus_sync_progress = models.FloatField(
        default=0.0,
        help_text="Consensus client sync progress (0-100)"
    )
    
    # Block/slot information
    current_block_height = models.BigIntegerField(
        null=True,
        blank=True,
        help_text="Current execution block height"
    )
    highest_block_height = models.BigIntegerField(
        null=True,
        blank=True,
        help_text="Target execution block height"
    )
    consensus_head_slot = models.BigIntegerField(
        null=True,
        blank=True,
        help_text="Current consensus slot"
    )
    
    # Status
    node_status = models.CharField(
        max_length=20,
        help_text="Node status at time of recording"
    )
    is_syncing = models.BooleanField(
        default=True,
        help_text="Whether node was actively syncing"
    )
    
    # Metrics
    peers_count = models.IntegerField(
        null=True,
        blank=True,
        help_text="Number of connected peers"
    )
    
    # Additional data
    metadata = models.JSONField(
        default=dict,
        blank=True,
        help_text="Additional metrics or error information"
    )
    
    class Meta:
        ordering = ['-timestamp']
        indexes = [
            models.Index(fields=['-timestamp']),
            models.Index(fields=['node', '-timestamp']),
        ]
        verbose_name = 'Sync Status History'
        verbose_name_plural = 'Sync Status Histories'
    
    def __str__(self):
        return f"{self.node.name} @ {self.timestamp:%Y-%m-%d %H:%M} - {self.execution_sync_progress:.1f}%"
    
    @classmethod
    def cleanup_old_records(cls, days=30):
        """Remove records older than specified days"""
        cutoff_date = timezone.now() - timedelta(days=days)
        deleted_count, _ = cls.objects.filter(timestamp__lt=cutoff_date).delete()
        return deleted_count
