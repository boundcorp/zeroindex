from django.db import models
from django.core.validators import URLValidator


class Chain(models.Model):
    chain_id = models.BigIntegerField(unique=True, db_index=True)
    name = models.CharField(max_length=255)
    symbol = models.CharField(max_length=10, default='ETH')
    is_testnet = models.BooleanField(default=False)
    block_explorer_url = models.URLField(blank=True, null=True)
    
    # Block chunking configuration
    chunk_duration_days = models.IntegerField(
        default=1, 
        help_text="Duration of each chunk in days (default: 1 day)"
    )
    estimated_blocks_per_day = models.IntegerField(
        default=7200,
        help_text="Estimated blocks produced per day (Ethereum: ~7200 blocks/day)"
    )
    average_block_time_seconds = models.FloatField(
        default=12.0,
        help_text="Average time between blocks in seconds (Ethereum: ~12 seconds)"
    )
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['chain_id']
        verbose_name = 'Blockchain'
        verbose_name_plural = 'Blockchains'

    def __str__(self):
        return f"{self.name} ({self.chain_id})"


class RpcProvider(models.Model):
    chain = models.ForeignKey(
        Chain, 
        on_delete=models.CASCADE, 
        related_name='rpc_providers'
    )
    rpc_url = models.URLField(validators=[URLValidator()])
    enabled = models.BooleanField(default=True)
    priority = models.IntegerField(default=0, help_text="Higher priority providers are preferred")
    rate_limit = models.IntegerField(
        null=True, 
        blank=True, 
        help_text="Requests per second limit"
    )
    last_health_check = models.DateTimeField(null=True, blank=True)
    is_healthy = models.BooleanField(default=True)
    response_time_ms = models.IntegerField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['chain', '-priority', 'rpc_url']
        verbose_name = 'RPC Provider'
        verbose_name_plural = 'RPC Providers'
        indexes = [
            models.Index(fields=['chain', 'enabled', '-priority']),
        ]

    def __str__(self):
        return f"{self.chain.name} - {self.rpc_url} ({'enabled' if self.enabled else 'disabled'})"
