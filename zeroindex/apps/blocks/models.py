from django.db import models
from django.contrib.postgres.fields import ArrayField
from zeroindex.apps.chains.models import Chain


class Block(models.Model):
    chain = models.ForeignKey(
        Chain,
        on_delete=models.CASCADE,
        related_name='blocks'
    )
    block_number = models.BigIntegerField(db_index=True)
    block_hash = models.CharField(max_length=66, db_index=True)
    parent_hash = models.CharField(max_length=66)
    timestamp = models.DateTimeField(db_index=True)
    
    miner = models.CharField(max_length=42, blank=True, null=True)
    difficulty = models.CharField(max_length=100, blank=True, null=True)
    total_difficulty = models.CharField(max_length=100, blank=True, null=True)
    gas_limit = models.BigIntegerField()
    gas_used = models.BigIntegerField()
    base_fee_per_gas = models.BigIntegerField(null=True, blank=True)
    
    transaction_count = models.IntegerField(default=0)
    transactions_root = models.CharField(max_length=66, blank=True)
    state_root = models.CharField(max_length=66, blank=True)
    receipts_root = models.CharField(max_length=66, blank=True)
    
    size = models.IntegerField(null=True, blank=True)
    extra_data = models.TextField(blank=True)
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['-block_number']
        verbose_name = 'Block'
        verbose_name_plural = 'Blocks'
        unique_together = [['chain', 'block_number'], ['chain', 'block_hash']]
        indexes = [
            models.Index(fields=['chain', '-block_number']),
            models.Index(fields=['timestamp']),
        ]

    def __str__(self):
        return f"Block {self.block_number} on {self.chain.name}"


class Transaction(models.Model):
    chain = models.ForeignKey(
        Chain,
        on_delete=models.CASCADE,
        related_name='transactions'
    )
    block = models.ForeignKey(
        Block,
        on_delete=models.CASCADE,
        related_name='transactions'
    )
    
    transaction_hash = models.CharField(max_length=66, unique=True, db_index=True)
    transaction_index = models.IntegerField()
    from_address = models.CharField(max_length=42, db_index=True)
    to_address = models.CharField(max_length=42, null=True, blank=True, db_index=True)
    
    value = models.CharField(max_length=100)
    gas = models.BigIntegerField()
    gas_price = models.BigIntegerField(null=True, blank=True)
    max_fee_per_gas = models.BigIntegerField(null=True, blank=True)
    max_priority_fee_per_gas = models.BigIntegerField(null=True, blank=True)
    
    nonce = models.BigIntegerField()
    input_data = models.TextField(blank=True)
    
    status = models.BooleanField(null=True, blank=True)
    gas_used = models.BigIntegerField(null=True, blank=True)
    effective_gas_price = models.BigIntegerField(null=True, blank=True)
    
    contract_address = models.CharField(max_length=42, null=True, blank=True, db_index=True)
    logs_count = models.IntegerField(default=0)
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['block', 'transaction_index']
        verbose_name = 'Transaction'
        verbose_name_plural = 'Transactions'
        indexes = [
            models.Index(fields=['chain', 'from_address']),
            models.Index(fields=['chain', 'to_address']),
            models.Index(fields=['chain', 'contract_address']),
        ]

    def __str__(self):
        return f"Tx {self.transaction_hash[:10]}..."


class Log(models.Model):
    chain = models.ForeignKey(
        Chain,
        on_delete=models.CASCADE,
        related_name='logs'
    )
    block = models.ForeignKey(
        Block,
        on_delete=models.CASCADE,
        related_name='logs'
    )
    transaction = models.ForeignKey(
        Transaction,
        on_delete=models.CASCADE,
        related_name='logs'
    )
    
    log_index = models.IntegerField()
    address = models.CharField(max_length=42, db_index=True)
    topics = ArrayField(
        models.CharField(max_length=66),
        size=4,
        blank=True,
        default=list
    )
    data = models.TextField(blank=True)
    
    removed = models.BooleanField(default=False)
    
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['block', 'transaction', 'log_index']
        verbose_name = 'Event Log'
        verbose_name_plural = 'Event Logs'
        unique_together = [['chain', 'block', 'transaction', 'log_index']]
        indexes = [
            models.Index(fields=['chain', 'address']),
            models.Index(fields=['topics']),
        ]

    def __str__(self):
        return f"Log {self.log_index} from {self.address[:10]}..."
