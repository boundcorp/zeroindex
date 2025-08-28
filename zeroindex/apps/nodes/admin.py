from django.contrib import admin
from django.utils.html import format_html
from django.urls import reverse
from django.utils.safestring import mark_safe
from .models import KubeCredential, Node
from .services import KubernetesNodeManager


@admin.register(KubeCredential)
class KubeCredentialAdmin(admin.ModelAdmin):
    list_display = ('name', 'cluster_name', 'namespace', 'is_active', 'created_at')
    list_filter = ('is_active', 'cluster_name', 'created_at')
    search_fields = ('name', 'cluster_name', 'namespace')
    readonly_fields = ('created_at', 'updated_at')
    
    fieldsets = (
        (None, {
            'fields': ('name', 'cluster_name', 'namespace', 'is_active')
        }),
        ('Configuration', {
            'fields': ('kubeconfig',),
            'classes': ('collapse',)
        }),
        ('Timestamps', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )


@admin.register(Node)
class NodeAdmin(admin.ModelAdmin):
    list_display = (
        'name', 
        'chain', 
        'status_badge', 
        'execution_client', 
        'consensus_client_display',
        'sync_progress_display',
        'current_block_height',
        'created_at'
    )
    list_filter = (
        'status', 
        'chain', 
        'execution_client', 
        'consensus_client',
        'node_type',
        'created_at'
    )
    search_fields = ('name', 'chain__name', 'execution_client', 'consensus_client')
    readonly_fields = (
        'created_at', 
        'updated_at', 
        'last_health_check',
        'deployment_info',
        'sync_status_display'
    )
    
    fieldsets = (
        (None, {
            'fields': ('name', 'chain', 'node_type', 'status')
        }),
        ('Client Configuration', {
            'fields': (
                'execution_client', 'execution_version',
                'consensus_client', 'consensus_version'
            )
        }),
        ('Kubernetes Deployment', {
            'fields': (
                'kube_credential',
                'execution_deployment_name', 'consensus_deployment_name',
                'execution_pvc_name', 'consensus_pvc_name'
            )
        }),
        ('Storage Configuration', {
            'fields': ('storage_size_gb', 'consensus_storage_size_gb')
        }),
        ('Network Endpoints', {
            'fields': (
                'execution_rpc_url', 'execution_ws_url', 'consensus_api_url'
            ),
            'classes': ('collapse',)
        }),
        ('Resource Configuration', {
            'fields': ('resource_requests', 'resource_limits', 'extra_args'),
            'classes': ('collapse',)
        }),
        ('Sync Status', {
            'fields': ('sync_status_display',),
        }),
        ('Deployment Info', {
            'fields': ('deployment_info',),
        }),
        ('Timestamps', {
            'fields': ('created_at', 'updated_at', 'last_health_check'),
            'classes': ('collapse',)
        }),
    )
    
    actions = ['start_nodes', 'stop_nodes', 'monitor_sync']
    
    def status_badge(self, obj):
        """Display status with colored badge"""
        colors = {
            'pending': 'gray',
            'provisioning': 'blue',
            'syncing': 'orange',
            'running': 'green',
            'error': 'red',
            'stopped': 'gray',
        }
        color = colors.get(obj.status, 'gray')
        return format_html(
            '<span style="background: {}; color: white; padding: 2px 6px; '
            'border-radius: 3px; font-size: 11px;">{}</span>',
            color, obj.status.upper()
        )
    status_badge.short_description = 'Status'
    
    def consensus_client_display(self, obj):
        """Show consensus client only for Ethereum L1"""
        if obj.is_ethereum_l1:
            return obj.consensus_client
        return '-'
    consensus_client_display.short_description = 'Consensus Client'
    
    def sync_progress_display(self, obj):
        """Display sync progress as progress bar"""
        if obj.is_ethereum_l1:
            exec_pct = obj.execution_sync_progress
            cons_pct = obj.consensus_sync_progress
            overall_pct = obj.overall_sync_progress
            
            return format_html(
                '<div title="Execution: {:.1f}% | Consensus: {:.1f}% | Overall: {:.1f}%">'
                '<div style="width: 100px; background: #f0f0f0; border-radius: 3px;">'
                '<div style="width: {}px; height: 16px; background: linear-gradient(to right, #4CAF50, #2196F3); '
                'border-radius: 3px; text-align: center; line-height: 16px; font-size: 10px; color: white;">'
                '{:.1f}%</div></div></div>',
                exec_pct, cons_pct, overall_pct,
                int(overall_pct), overall_pct
            )
        else:
            exec_pct = obj.execution_sync_progress
            return format_html(
                '<div style="width: 100px; background: #f0f0f0; border-radius: 3px;">'
                '<div style="width: {}px; height: 16px; background: #4CAF50; '
                'border-radius: 3px; text-align: center; line-height: 16px; font-size: 10px; color: white;">'
                '{:.1f}%</div></div>',
                int(exec_pct), exec_pct
            )
    sync_progress_display.short_description = 'Sync Progress'
    
    def sync_status_display(self, obj):
        """Display detailed sync status"""
        html = []
        
        if obj.is_ethereum_l1:
            html.append(f"<strong>Execution Client:</strong> {obj.execution_sync_progress:.1f}%")
            html.append(f"<strong>Consensus Client:</strong> {obj.consensus_sync_progress:.1f}%")
            html.append(f"<strong>Overall Progress:</strong> {obj.overall_sync_progress:.1f}%")
        else:
            html.append(f"<strong>Sync Progress:</strong> {obj.execution_sync_progress:.1f}%")
        
        if obj.current_block_height:
            html.append(f"<strong>Current Block:</strong> {obj.current_block_height:,}")
        
        if obj.consensus_head_slot and obj.is_ethereum_l1:
            html.append(f"<strong>Consensus Slot:</strong> {obj.consensus_head_slot:,}")
        
        if obj.is_fully_synced:
            html.append('<span style="color: green; font-weight: bold;">✓ Fully Synced</span>')
        
        return mark_safe('<br>'.join(html))
    sync_status_display.short_description = 'Sync Status Details'
    
    def deployment_info(self, obj):
        """Display Kubernetes deployment information"""
        html = []
        
        if obj.kube_credential:
            html.append(f"<strong>Cluster:</strong> {obj.kube_credential.cluster_name}")
            html.append(f"<strong>Namespace:</strong> {obj.kube_credential.namespace}")
        
        if obj.execution_deployment_name:
            html.append(f"<strong>Execution Deployment:</strong> {obj.execution_deployment_name}")
        
        if obj.consensus_deployment_name and obj.is_ethereum_l1:
            html.append(f"<strong>Consensus Deployment:</strong> {obj.consensus_deployment_name}")
        
        if obj.execution_pvc_name:
            html.append(f"<strong>Execution PVC:</strong> {obj.execution_pvc_name}")
        
        if obj.consensus_pvc_name and obj.is_ethereum_l1:
            html.append(f"<strong>Consensus PVC:</strong> {obj.consensus_pvc_name}")
        
        # Add Kubernetes status if available
        if obj.kube_credential and obj.kube_credential.is_active:
            try:
                k8s_manager = KubernetesNodeManager(obj.kube_credential)
                k8s_status = k8s_manager.get_node_status(obj)
                
                exec_status = k8s_status.get('execution_client')
                if exec_status and 'error' not in exec_status:
                    ready = exec_status.get('ready_replicas', 0)
                    total = exec_status.get('replicas', 0)
                    html.append(f"<strong>Execution Status:</strong> {ready}/{total} ready")
                
                cons_status = k8s_status.get('consensus_client')
                if cons_status and 'error' not in cons_status:
                    ready = cons_status.get('ready_replicas', 0)
                    total = cons_status.get('replicas', 0)
                    html.append(f"<strong>Consensus Status:</strong> {ready}/{total} ready")
                    
            except Exception as e:
                html.append(f'<span style="color: red;">K8s Status Error: {str(e)}</span>')
        
        return mark_safe('<br>'.join(html)) if html else '-'
    deployment_info.short_description = 'Deployment Info'
    
    def start_nodes(self, request, queryset):
        """Admin action to start selected nodes"""
        started = 0
        errors = []
        
        for node in queryset:
            if not node.kube_credential or not node.kube_credential.is_active:
                errors.append(f"{node.name}: No active Kubernetes credentials")
                continue
            
            try:
                k8s_manager = KubernetesNodeManager(node.kube_credential)
                if k8s_manager.deploy_node(node):
                    started += 1
                else:
                    errors.append(f"{node.name}: Deployment failed")
            except Exception as e:
                errors.append(f"{node.name}: {str(e)}")
        
        if started:
            self.message_user(request, f"Successfully started {started} nodes")
        
        if errors:
            self.message_user(request, f"Errors: {'; '.join(errors)}", level='ERROR')
    
    start_nodes.short_description = "Start selected nodes"
    
    def stop_nodes(self, request, queryset):
        """Admin action to stop selected nodes"""
        stopped = 0
        errors = []
        
        for node in queryset:
            if not node.kube_credential or not node.kube_credential.is_active:
                errors.append(f"{node.name}: No active Kubernetes credentials")
                continue
            
            try:
                k8s_manager = KubernetesNodeManager(node.kube_credential)
                if k8s_manager.delete_node(node):
                    stopped += 1
                else:
                    errors.append(f"{node.name}: Stop failed")
            except Exception as e:
                errors.append(f"{node.name}: {str(e)}")
        
        if stopped:
            self.message_user(request, f"Successfully stopped {stopped} nodes")
        
        if errors:
            self.message_user(request, f"Errors: {'; '.join(errors)}", level='ERROR')
    
    stop_nodes.short_description = "Stop selected nodes"
    
    def monitor_sync(self, request, queryset):
        """Admin action to monitor sync status of selected nodes"""
        from .sync_monitor import monitor_node_sync
        
        monitored = 0
        errors = []
        
        for node in queryset:
            try:
                result = monitor_node_sync(node.name)
                if 'error' in result:
                    errors.append(f"{node.name}: {result['error']}")
                else:
                    monitored += 1
            except Exception as e:
                errors.append(f"{node.name}: {str(e)}")
        
        if monitored:
            self.message_user(request, f"Successfully monitored {monitored} nodes")
        
        if errors:
            self.message_user(request, f"Errors: {'; '.join(errors)}", level='ERROR')
    
    monitor_sync.short_description = "Monitor sync status"
