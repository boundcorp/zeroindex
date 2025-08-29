#!/usr/bin/env python3
"""
Chunk testing and validation utilities.
Quality assurance while soaring through the stratosphere.
"""

import json
import gzip
import hashlib
import time
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
from web3 import Web3, HTTPProvider
import asyncio
from dataclasses import dataclass
from enum import Enum


class ValidationResult(Enum):
    PASS = "PASS"
    FAIL = "FAIL" 
    PARTIAL = "PARTIAL"
    ERROR = "ERROR"


@dataclass
class TestResult:
    test_name: str
    result: ValidationResult
    details: Dict[str, Any]
    duration: float


class ChunkValidator:
    """
    Validates blockchain chunk integrity and correctness.
    Ensures data quality meets production standards.
    """
    
    def __init__(self, rpc_url: Optional[str] = None):
        """Initialize validator with blockchain connection."""
        self.rpc_url = rpc_url or os.getenv('ETH_RPC_URL', 'http://ethereum-l1-node:8545')
        self.w3 = Web3(HTTPProvider(self.rpc_url))
        self.test_results: List[TestResult] = []
        
    def load_chunk(self, chunk_file: Path) -> Dict[str, Any]:
        """Load and decompress a chunk file."""
        try:
            with gzip.open(chunk_file, 'rt', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            raise ValueError(f"Cannot load chunk {chunk_file}: {e}")
            
    def validate_chunk_structure(self, chunk_data: Dict[str, Any]) -> TestResult:
        """Validate basic chunk data structure."""
        start_time = time.time()
        
        required_fields = ['version', 'chain', 'start_block', 'end_block', 'blocks', 'metadata']
        missing_fields = []
        
        for field in required_fields:
            if field not in chunk_data:
                missing_fields.append(field)
                
        # Check blocks array
        blocks_valid = True
        block_issues = []
        
        if 'blocks' in chunk_data:
            for i, block in enumerate(chunk_data['blocks']):
                if not isinstance(block, dict):
                    block_issues.append(f"Block {i} is not a dictionary")
                    continue
                    
                # Check required block fields
                block_required = ['number', 'hash', 'timestamp', 'transactions']
                for field in block_required:
                    if field not in block:
                        block_issues.append(f"Block {i} missing {field}")
                        
        result = ValidationResult.PASS
        if missing_fields or block_issues:
            result = ValidationResult.FAIL if missing_fields else ValidationResult.PARTIAL
            
        return TestResult(
            test_name="chunk_structure",
            result=result,
            details={
                'missing_fields': missing_fields,
                'block_issues': block_issues,
                'block_count': len(chunk_data.get('blocks', []))
            },
            duration=time.time() - start_time
        )
        
    def validate_block_sequence(self, chunk_data: Dict[str, Any]) -> TestResult:
        """Validate blocks are in correct sequence."""
        start_time = time.time()
        
        blocks = chunk_data.get('blocks', [])
        if not blocks:
            return TestResult(
                test_name="block_sequence",
                result=ValidationResult.ERROR,
                details={'error': 'No blocks in chunk'},
                duration=time.time() - start_time
            )
            
        issues = []
        
        # Check block numbers are sequential
        for i in range(1, len(blocks)):
            prev_num = blocks[i-1].get('number', 0)
            curr_num = blocks[i].get('number', 0)
            
            if curr_num != prev_num + 1:
                issues.append(f"Block sequence gap: {prev_num} -> {curr_num}")
                
        # Check timestamps are ascending
        for i in range(1, len(blocks)):
            prev_time = blocks[i-1].get('timestamp', 0)
            curr_time = blocks[i].get('timestamp', 0)
            
            if curr_time < prev_time:
                issues.append(f"Timestamp out of order: {prev_time} -> {curr_time}")
                
        # Verify range matches metadata
        actual_start = blocks[0].get('number', 0)
        actual_end = blocks[-1].get('number', 0)
        expected_start = chunk_data.get('start_block', 0)
        expected_end = chunk_data.get('end_block', 0)
        
        if actual_start != expected_start:
            issues.append(f"Start block mismatch: expected {expected_start}, got {actual_start}")
        if actual_end != expected_end:
            issues.append(f"End block mismatch: expected {expected_end}, got {actual_end}")
            
        result = ValidationResult.PASS if not issues else ValidationResult.FAIL
        
        return TestResult(
            test_name="block_sequence",
            result=result,
            details={
                'issues': issues,
                'actual_range': f"{actual_start}-{actual_end}",
                'expected_range': f"{expected_start}-{expected_end}"
            },
            duration=time.time() - start_time
        )
        
    def validate_against_blockchain(self, chunk_data: Dict[str, Any], 
                                   sample_blocks: int = 3) -> TestResult:
        """Validate chunk data against live blockchain."""
        start_time = time.time()
        
        if not self.w3.is_connected():
            return TestResult(
                test_name="blockchain_validation",
                result=ValidationResult.ERROR,
                details={'error': 'Cannot connect to blockchain'},
                duration=time.time() - start_time
            )
            
        blocks = chunk_data.get('blocks', [])
        if not blocks:
            return TestResult(
                test_name="blockchain_validation",
                result=ValidationResult.ERROR,
                details={'error': 'No blocks to validate'},
                duration=time.time() - start_time
            )
            
        # Sample blocks to validate
        step = max(1, len(blocks) // sample_blocks)
        sample_indices = list(range(0, len(blocks), step))[:sample_blocks]
        
        validation_results = []
        
        for idx in sample_indices:
            block_data = blocks[idx]
            block_num = block_data.get('number')
            
            try:
                # Fetch from blockchain
                chain_block = self.w3.eth.get_block(block_num)
                
                # Compare key fields
                comparison = {
                    'block_number': block_num,
                    'hash_match': block_data.get('hash') == chain_block['hash'].hex(),
                    'parent_hash_match': block_data.get('parentHash') == chain_block['parentHash'].hex(),
                    'timestamp_match': block_data.get('timestamp') == chain_block['timestamp'],
                    'transaction_count_match': len(block_data.get('transactions', [])) == len(chain_block['transactions'])
                }
                
                validation_results.append(comparison)
                
            except Exception as e:
                validation_results.append({
                    'block_number': block_num,
                    'error': str(e)
                })
                
        # Analyze results
        successful_validations = sum(1 for r in validation_results if 'error' not in r)
        failed_fields = []
        
        for result in validation_results:
            if 'error' in result:
                continue
            for field, matches in result.items():
                if field != 'block_number' and not matches:
                    failed_fields.append(f"Block {result['block_number']}: {field}")
                    
        if successful_validations == 0:
            result = ValidationResult.ERROR
        elif failed_fields:
            result = ValidationResult.PARTIAL
        else:
            result = ValidationResult.PASS
            
        return TestResult(
            test_name="blockchain_validation",
            result=result,
            details={
                'validated_blocks': successful_validations,
                'total_sampled': len(sample_indices),
                'failed_fields': failed_fields,
                'validation_details': validation_results
            },
            duration=time.time() - start_time
        )
        
    def validate_transaction_traces(self, chunk_data: Dict[str, Any], 
                                  sample_txs: int = 5) -> TestResult:
        """Validate transaction trace data quality."""
        start_time = time.time()
        
        blocks = chunk_data.get('blocks', [])
        all_txs = []
        
        # Collect all transactions
        for block in blocks:
            all_txs.extend(block.get('transactions', []))
            
        if not all_txs:
            return TestResult(
                test_name="transaction_traces",
                result=ValidationResult.ERROR,
                details={'error': 'No transactions found'},
                duration=time.time() - start_time
            )
            
        # Sample transactions
        step = max(1, len(all_txs) // sample_txs)
        sample_txs_data = all_txs[::step][:sample_txs]
        
        trace_stats = {
            'total_txs': len(all_txs),
            'sampled_txs': len(sample_txs_data),
            'txs_with_traces': 0,
            'txs_with_internal_calls': 0,
            'txs_with_receipts': 0,
            'trace_types': set()
        }
        
        for tx in sample_txs_data:
            # Check for trace data
            if tx.get('trace'):
                trace_stats['txs_with_traces'] += 1
                
                trace = tx['trace']
                if isinstance(trace, dict) and 'calls' in trace:
                    trace_stats['txs_with_internal_calls'] += 1
                    trace_stats['trace_types'].add('debug_trace')
                elif isinstance(trace, list):
                    trace_stats['trace_types'].add('parity_trace')
                    
            # Check for receipt data
            if tx.get('receipt'):
                trace_stats['txs_with_receipts'] += 1
                
        trace_coverage = trace_stats['txs_with_traces'] / trace_stats['sampled_txs']
        
        if trace_coverage > 0.8:
            result = ValidationResult.PASS
        elif trace_coverage > 0.5:
            result = ValidationResult.PARTIAL
        else:
            result = ValidationResult.FAIL
            
        trace_stats['trace_types'] = list(trace_stats['trace_types'])
        trace_stats['trace_coverage'] = trace_coverage
        
        return TestResult(
            test_name="transaction_traces",
            result=result,
            details=trace_stats,
            duration=time.time() - start_time
        )
        
    def validate_compression_integrity(self, chunk_file: Path) -> TestResult:
        """Validate file compression and checksums."""
        start_time = time.time()
        
        # Load metadata if available
        meta_file = chunk_file.with_suffix(chunk_file.suffix + '.meta')
        metadata = {}
        
        if meta_file.exists():
            with open(meta_file, 'r') as f:
                metadata = json.load(f)
                
        # Calculate actual file hash
        with open(chunk_file, 'rb') as f:
            file_data = f.read()
            actual_hash = hashlib.sha256(file_data).hexdigest()
            
        integrity_check = {
            'file_size': len(file_data),
            'calculated_hash': actual_hash,
            'metadata_hash': metadata.get('sha256', 'missing'),
            'hash_match': actual_hash == metadata.get('sha256'),
            'compression_ratio': metadata.get('compression_ratio', 'unknown')
        }
        
        # Try to decompress
        try:
            with gzip.open(chunk_file, 'rt') as f:
                decompressed = f.read()
                integrity_check['decompression_success'] = True
                integrity_check['decompressed_size'] = len(decompressed)
        except Exception as e:
            integrity_check['decompression_success'] = False
            integrity_check['decompression_error'] = str(e)
            
        if integrity_check['hash_match'] and integrity_check['decompression_success']:
            result = ValidationResult.PASS
        elif integrity_check['decompression_success']:
            result = ValidationResult.PARTIAL
        else:
            result = ValidationResult.FAIL
            
        return TestResult(
            test_name="compression_integrity",
            result=result,
            details=integrity_check,
            duration=time.time() - start_time
        )
        
    def run_full_validation(self, chunk_file: Path) -> Dict[str, Any]:
        """Run complete validation suite on a chunk file."""
        print(f"✈️ Running full validation on {chunk_file.name} at 30,000 feet")
        
        # Load chunk data
        try:
            chunk_data = self.load_chunk(chunk_file)
        except Exception as e:
            return {
                'file': str(chunk_file),
                'success': False,
                'error': f"Cannot load chunk: {e}",
                'tests': []
            }
            
        # Run all validation tests
        tests = [
            self.validate_chunk_structure(chunk_data),
            self.validate_block_sequence(chunk_data),
            self.validate_compression_integrity(chunk_file),
            self.validate_transaction_traces(chunk_data),
            self.validate_against_blockchain(chunk_data)
        ]
        
        # Calculate overall result
        pass_count = sum(1 for t in tests if t.result == ValidationResult.PASS)
        fail_count = sum(1 for t in tests if t.result == ValidationResult.FAIL)
        
        overall_success = fail_count == 0
        
        return {
            'file': str(chunk_file),
            'success': overall_success,
            'summary': {
                'pass': pass_count,
                'fail': fail_count,
                'partial': sum(1 for t in tests if t.result == ValidationResult.PARTIAL),
                'error': sum(1 for t in tests if t.result == ValidationResult.ERROR),
                'total_duration': sum(t.duration for t in tests)
            },
            'tests': [
                {
                    'name': t.test_name,
                    'result': t.result.value,
                    'details': t.details,
                    'duration': t.duration
                }
                for t in tests
            ]
        }
        
    async def validate_time_range(self, chunks_dir: Path, 
                                start_time: datetime, end_time: datetime) -> Dict[str, Any]:
        """Validate all chunks within a specific time range."""
        print(f"✈️ Validating chunks from {start_time} to {end_time}")
        
        chunk_files = list(chunks_dir.glob('*.json.gz'))
        relevant_chunks = []
        
        # Filter chunks by time range
        for chunk_file in chunk_files:
            try:
                chunk_data = self.load_chunk(chunk_file)
                chunk_start_time = datetime.fromtimestamp(
                    chunk_data.get('metadata', {}).get('start_timestamp', 0)
                )
                
                if start_time <= chunk_start_time <= end_time:
                    relevant_chunks.append(chunk_file)
            except Exception:
                continue
                
        print(f"Found {len(relevant_chunks)} chunks in time range")
        
        # Validate each chunk
        results = []
        for chunk_file in relevant_chunks:
            result = self.run_full_validation(chunk_file)
            results.append(result)
            
            status = "✅" if result['success'] else "❌"
            print(f"{status} {chunk_file.name}")
            
        # Summary statistics
        successful = sum(1 for r in results if r['success'])
        
        return {
            'time_range': {
                'start': start_time.isoformat(),
                'end': end_time.isoformat()
            },
            'chunks_found': len(relevant_chunks),
            'chunks_validated': len(results),
            'successful_validations': successful,
            'success_rate': successful / len(results) if results else 0,
            'results': results
        }


async def main():
    """Main testing routine - quality control at altitude."""
    validator = ChunkValidator()
    
    print("✈️ Chunk Validator initialized at cruising altitude")
    print("=" * 60)
    
    # Look for test chunks
    chunks_dir = Path('./data/blocks/chunks')
    if not chunks_dir.exists():
        print("📁 No chunks directory found - create some chunks first")
        return
        
    chunk_files = list(chunks_dir.glob('*.json.gz'))
    
    if not chunk_files:
        print("📦 No chunk files found for testing")
        return
        
    print(f"🧪 Found {len(chunk_files)} chunks to validate")
    
    # Validate first few chunks
    test_files = chunk_files[:3]
    
    for chunk_file in test_files:
        print(f"\n🔍 Validating {chunk_file.name}...")
        result = validator.run_full_validation(chunk_file)
        
        status = "✅ PASSED" if result['success'] else "❌ FAILED"
        print(f"{status} - {result['summary']}")
        
    print("\n🎯 Validation complete!")


if __name__ == "__main__":
    asyncio.run(main())