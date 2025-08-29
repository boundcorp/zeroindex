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


class Chunk(models.Model):
    STATUS_CHOICES = [
        ('creating', 'Creating'),
        ('complete', 'Complete'),
        ('incomplete', 'Incomplete'),
        ('repairing', 'Repairing'),
        ('failed', 'Failed'),
    ]
    
    chain = models.ForeignKey(Chain, on_delete=models.CASCADE, related_name='chunks')
    start_block = models.BigIntegerField(db_index=True)
    end_block = models.BigIntegerField(db_index=True)
    
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='creating')
    completeness_percentage = models.DecimalField(max_digits=5, decimal_places=2, default=0.00)
    missing_blocks = models.JSONField(default=list)
    
    # File information
    file_path = models.TextField(blank=True)
    file_size_bytes = models.BigIntegerField(default=0)
    compression_ratio = models.FloatField(default=1.0)
    file_hash = models.CharField(max_length=64, blank=True)
    
    # Stats
    total_blocks = models.IntegerField(default=0)
    total_transactions = models.IntegerField(default=0)
    
    # Timestamps
    chunk_date = models.DateField(db_index=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    last_repair_attempt = models.DateTimeField(null=True, blank=True)
    
    class Meta:
        ordering = ['-chunk_date', '-start_block']
        verbose_name = 'Chunk'
        verbose_name_plural = 'Chunks'
        unique_together = [['chain', 'start_block', 'end_block']]
        indexes = [
            models.Index(fields=['chain', '-chunk_date']),
            models.Index(fields=['status']),
            models.Index(fields=['completeness_percentage']),
        ]

    def __str__(self):
        return f"Chunk {self.start_block}-{self.end_block} ({self.completeness_percentage}% complete)"
    
    def analyze_missing_blocks(self):
        """Analyze which blocks are missing from this chunk"""
        if not self.file_path or not self.file_path.endswith('.json.gz'):
            return []
            
        import json
        import gzip
        
        try:
            with gzip.open(self.file_path, 'rt') as f:
                chunk_data = json.load(f)
            
            blocks = chunk_data.get('blocks', [])
            existing_block_numbers = {int(block['number']) for block in blocks}
            
            missing_blocks = [
                block_num for block_num in range(self.start_block, self.end_block + 1)
                if block_num not in existing_block_numbers
            ]
            
            # Update the missing blocks field
            self.missing_blocks = missing_blocks
            expected_blocks = self.end_block - self.start_block + 1
            actual_blocks = len(existing_block_numbers)
            self.completeness_percentage = (actual_blocks / expected_blocks) * 100 if expected_blocks > 0 else 0
            self.total_blocks = actual_blocks
            self.save()
            
            return missing_blocks
            
        except Exception as e:
            print(f"Error analyzing chunk: {e}")
            return []
    
    def repair_missing_blocks(self):
        """Attempt to repair missing blocks in this chunk"""
        from datetime import datetime
        
        missing_blocks = self.analyze_missing_blocks()
        if not missing_blocks:
            return None
            
        self.status = 'repairing'
        self.last_repair_attempt = datetime.now()
        self.save()
        
        # Create repair log
        repair_log = ChunkRepairLog.objects.create(
            chunk=self,
            blocks_attempted=len(missing_blocks),
            missing_blocks_before=missing_blocks.copy()
        )
        
        try:
            # Import Web3 and connect to chain
            from web3 import Web3
            import json
            import gzip
            
            # Get RPC URL from chain
            w3 = Web3(Web3.HTTPProvider(self.chain.rpc_url))
            if not w3.is_connected():
                repair_log.error_message = "Cannot connect to RPC"
                repair_log.save()
                self.status = 'failed'
                self.save()
                return repair_log
            
            # Load existing chunk data
            with gzip.open(self.file_path, 'rt') as f:
                chunk_data = json.load(f)
            
            blocks_repaired = 0
            new_blocks = []
            
            for block_num in missing_blocks[:5]:  # Limit to 5 blocks per repair attempt
                try:
                    block = w3.eth.get_block(block_num, full_transactions=True)
                    
                    # Convert block to our format
                    block_data = {
                        'number': block['number'],
                        'hash': block['hash'].hex(),
                        'parent_hash': block['parentHash'].hex(),
                        'timestamp': block['timestamp'],
                        'miner': block.get('miner', ''),
                        'gas_limit': block['gasLimit'],
                        'gas_used': block['gasUsed'],
                        'base_fee_per_gas': block.get('baseFeePerGas'),
                        'transaction_count': len(block['transactions']),
                        'transactions': []
                    }
                    
                    # Add transactions
                    for tx in block['transactions']:
                        tx_data = {
                            'hash': tx['hash'].hex(),
                            'from': tx['from'],
                            'to': tx.get('to', ''),
                            'value': str(tx['value']),
                            'gas': tx['gas'],
                            'gas_price': str(tx.get('gasPrice', 0)),
                            'nonce': tx['nonce'],
                            'transaction_index': tx['transactionIndex']
                        }
                        block_data['transactions'].append(tx_data)
                    
                    new_blocks.append(block_data)
                    blocks_repaired += 1
                    
                except Exception as e:
                    print(f"Error fetching block {block_num}: {e}")
                    continue
            
            if new_blocks:
                # Add new blocks to chunk data
                chunk_data['blocks'].extend(new_blocks)
                chunk_data['blocks'].sort(key=lambda x: x['number'])
                
                # Update metadata
                chunk_data['metadata']['total_blocks'] = len(chunk_data['blocks'])
                chunk_data['metadata']['last_repair'] = datetime.now().isoformat()
                
                # Save updated chunk
                with gzip.open(self.file_path, 'wt') as f:
                    json.dump(chunk_data, f)
                
                # Update chunk status
                self.analyze_missing_blocks()  # Recalculate completeness
                
                if self.completeness_percentage == 100:
                    self.status = 'complete'
                else:
                    self.status = 'incomplete'
            
            # Update repair log
            repair_log.blocks_repaired = blocks_repaired
            repair_log.missing_blocks_after = self.missing_blocks
            repair_log.completed_at = datetime.now()
            repair_log.save()
            
            self.save()
            return repair_log
            
        except Exception as e:
            repair_log.error_message = str(e)
            repair_log.save()
            self.status = 'failed'
            self.save()
            return repair_log


class ChunkRepairLog(models.Model):
    chunk = models.ForeignKey(Chunk, on_delete=models.CASCADE, related_name='repair_logs')
    
    # Repair attempt info
    started_at = models.DateTimeField(auto_now_add=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    
    # Statistics
    blocks_attempted = models.IntegerField(default=0)
    blocks_repaired = models.IntegerField(default=0)
    
    # Before/after comparison
    missing_blocks_before = models.JSONField(default=list)
    missing_blocks_after = models.JSONField(default=list)
    
    # Error handling
    error_message = models.TextField(blank=True)
    
    class Meta:
        ordering = ['-started_at']
        verbose_name = 'Chunk Repair Log'
        verbose_name_plural = 'Chunk Repair Logs'

    def __str__(self):
        return f"Repair {self.chunk} - {self.blocks_repaired}/{self.blocks_attempted} blocks"
