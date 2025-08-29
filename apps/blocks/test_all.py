#!/usr/bin/env python3
"""
Comprehensive tests for the Blocks app.
Testing at ground level before we fly again.
"""

import pytest
import json
import gzip
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import hashlib
from datetime import datetime

from trace_verifier import TraceVerifier
from block_chunker import BlockChunker
from s3_uploader import S3Uploader
from test_chunks import ChunkValidator, ValidationResult, TestResult


class TestTraceVerifier:
    """Test trace verification functionality."""
    
    def test_init(self):
        verifier = TraceVerifier('http://test:8545')
        assert verifier.rpc_url == 'http://test:8545'
        assert verifier.w3 is not None
        
    @patch('trace_verifier.Web3')
    def test_check_connection_success(self, mock_web3):
        mock_web3_instance = Mock()
        mock_web3_instance.is_connected.return_value = True
        mock_web3.return_value = mock_web3_instance
        
        verifier = TraceVerifier()
        verifier.w3 = mock_web3_instance
        assert verifier.check_connection() == True
        
    @patch('trace_verifier.Web3')
    def test_check_connection_failure(self, mock_web3):
        mock_web3_instance = Mock()
        mock_web3_instance.is_connected.side_effect = Exception("Connection failed")
        mock_web3.return_value = mock_web3_instance
        
        verifier = TraceVerifier()
        verifier.w3 = mock_web3_instance
        assert verifier.check_connection() == False
        
    @patch('trace_verifier.Web3')
    def test_get_latest_block(self, mock_web3):
        mock_web3_instance = Mock()
        mock_web3_instance.eth.block_number = 12345
        mock_web3.return_value = mock_web3_instance
        
        verifier = TraceVerifier()
        verifier.w3 = mock_web3_instance
        assert verifier.get_latest_block() == 12345
        
    @patch('trace_verifier.Web3')
    def test_debug_trace_block_success(self, mock_web3):
        mock_web3_instance = Mock()
        mock_provider = Mock()
        mock_provider.make_request.return_value = {
            'result': [
                {'result': {'calls': []}},
                {'result': {'calls': [{'type': 'call'}]}}
            ]
        }
        mock_web3_instance.provider = mock_provider
        
        verifier = TraceVerifier()
        verifier.w3 = mock_web3_instance
        
        result = verifier.check_debug_trace_block(12345)
        
        assert result['available'] == True
        assert result['trace_count'] == 2
        assert result['has_internal_calls'] == True
        
    def test_count_call_depth(self):
        verifier = TraceVerifier()
        
        # No calls
        trace = {}
        assert verifier._count_call_depth(trace) == 0
        
        # One level of calls
        trace = {'calls': [{'type': 'call'}, {'type': 'call'}]}
        assert verifier._count_call_depth(trace) == 1
        
        # Nested calls (depth should be 2: root -> calls[0] -> calls[0])
        trace = {
            'calls': [
                {'calls': [{'calls': []}]},
                {'type': 'call'}
            ]
        }
        assert verifier._count_call_depth(trace) == 2


class TestBlockChunker:
    """Test block chunking functionality."""
    
    def test_init(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            chunker = BlockChunker(data_dir=tmpdir)
            assert chunker.data_dir.exists()
            assert (chunker.data_dir / 'raw').exists()
            assert (chunker.data_dir / 'chunks').exists()
            
    @patch('block_chunker.Web3')
    def test_get_block_with_traces(self, mock_web3):
        mock_web3_instance = Mock()
        
        # Mock block data
        mock_block = {
            'number': 12345,
            'hash': Mock(hex=Mock(return_value='0xabcd')),
            'parentHash': Mock(hex=Mock(return_value='0x1234')),
            'timestamp': 1640995200,
            'miner': '0xminer',
            'difficulty': 1000,
            'size': 1024,
            'gasUsed': 100000,
            'gasLimit': 300000,
            'transactions': [
                {
                    'hash': Mock(hex=Mock(return_value='0xtx1')),
                    'from': '0xfrom',
                    'to': '0xto',
                    'value': 1000000000000000000,
                    'gas': 21000,
                    'nonce': 1,
                    'input': '0x'
                }
            ],
            'uncles': []
        }
        
        mock_web3_instance.eth.get_block.return_value = mock_block
        
        # Mock receipts
        mock_provider = Mock()
        mock_provider.make_request.return_value = {
            'result': [
                {
                    'transactionHash': '0xtx1',
                    'gasUsed': 21000,
                    'status': 1,
                    'logs': []
                }
            ]
        }
        mock_web3_instance.provider = mock_provider
        
        chunker = BlockChunker()
        chunker.w3 = mock_web3_instance
        
        result = chunker.get_block_with_traces(12345)
        
        assert result['number'] == 12345
        assert result['hash'] == '0xabcd'
        assert len(result['transactions']) == 1
        assert result['transactions'][0]['hash'] == '0xtx1'
        
    def test_compress_chunk(self):
        chunker = BlockChunker()
        
        chunk_data = {
            'version': '1.0',
            'start_block': 100,
            'end_block': 110,
            'blocks': [
                {'number': 100, 'transactions': []},
                {'number': 101, 'transactions': []}
            ]
        }
        
        compressed, metadata = chunker.compress_chunk(chunk_data)
        
        assert isinstance(compressed, bytes)
        assert metadata['original_size'] > 0
        assert metadata['compressed_size'] > 0
        assert metadata['compression_ratio'] > 1
        assert metadata['blocks'] == '100-110'
        assert 'sha256' in metadata
        
        # Verify we can decompress
        decompressed = gzip.decompress(compressed).decode('utf-8')
        restored = json.loads(decompressed)
        assert restored == chunk_data


class TestS3Uploader:
    """Test S3 upload functionality."""
    
    def test_init(self):
        uploader = S3Uploader('test-bucket')
        assert uploader.bucket_name == 'test-bucket'
        assert uploader.prefix == 'ethereum/blocks'
        
    def test_generate_s3_key(self):
        uploader = S3Uploader('test-bucket')
        
        # Test with timestamp
        key = uploader.generate_s3_key('test_file.json.gz', 1640995200)
        assert key.startswith('ethereum/blocks/2022/01/01')
        assert key.endswith('test_file.json.gz')
        
    @patch('s3_uploader.boto3')
    def test_check_credentials_success(self, mock_boto3):
        mock_session = Mock()
        mock_s3 = Mock()
        mock_s3.head_bucket.return_value = {}
        mock_session.client.return_value = mock_s3
        mock_boto3.Session.return_value = mock_session
        
        uploader = S3Uploader('test-bucket')
        uploader.s3 = mock_s3
        
        assert uploader.check_credentials() == True
        assert uploader.bucket_exists == True
        
    @patch('s3_uploader.boto3')
    def test_check_credentials_failure(self, mock_boto3):
        from botocore.exceptions import ClientError
        
        mock_session = Mock()
        mock_s3 = Mock()
        mock_s3.head_bucket.side_effect = ClientError(
            {'Error': {'Code': '404'}}, 'head_bucket'
        )
        mock_session.client.return_value = mock_s3
        mock_boto3.Session.return_value = mock_session
        
        uploader = S3Uploader('test-bucket')
        uploader.s3 = mock_s3
        
        assert uploader.check_credentials() == False


class TestChunkValidator:
    """Test chunk validation functionality."""
    
    def test_init(self):
        validator = ChunkValidator('http://test:8545')
        assert validator.rpc_url == 'http://test:8545'
        
    def test_validate_chunk_structure_success(self):
        validator = ChunkValidator()
        
        chunk_data = {
            'version': '1.0',
            'chain': 'ethereum',
            'start_block': 100,
            'end_block': 110,
            'blocks': [
                {
                    'number': 100,
                    'hash': '0xabc',
                    'timestamp': 1640995200,
                    'transactions': []
                }
            ],
            'metadata': {}
        }
        
        result = validator.validate_chunk_structure(chunk_data)
        
        assert result.test_name == 'chunk_structure'
        assert result.result == ValidationResult.PASS
        assert result.details['missing_fields'] == []
        assert result.details['block_issues'] == []
        
    def test_validate_chunk_structure_failure(self):
        validator = ChunkValidator()
        
        chunk_data = {
            'version': '1.0',
            'blocks': [
                {
                    'number': 100
                    # Missing required fields
                }
            ]
        }
        
        result = validator.validate_chunk_structure(chunk_data)
        
        assert result.result == ValidationResult.FAIL
        assert 'chain' in result.details['missing_fields']
        assert len(result.details['block_issues']) > 0
        
    def test_validate_block_sequence_success(self):
        validator = ChunkValidator()
        
        chunk_data = {
            'start_block': 100,
            'end_block': 102,
            'blocks': [
                {'number': 100, 'timestamp': 1000},
                {'number': 101, 'timestamp': 1012},
                {'number': 102, 'timestamp': 1024}
            ]
        }
        
        result = validator.validate_block_sequence(chunk_data)
        
        assert result.result == ValidationResult.PASS
        assert result.details['issues'] == []
        
    def test_validate_block_sequence_failure(self):
        validator = ChunkValidator()
        
        chunk_data = {
            'start_block': 100,
            'end_block': 102,
            'blocks': [
                {'number': 100, 'timestamp': 1000},
                {'number': 102, 'timestamp': 1024},  # Gap in sequence
                {'number': 103, 'timestamp': 1036}   # Wrong end block
            ]
        }
        
        result = validator.validate_block_sequence(chunk_data)
        
        assert result.result == ValidationResult.FAIL
        assert len(result.details['issues']) > 0
        
    def test_validate_compression_integrity(self):
        validator = ChunkValidator()
        
        # Create test chunk file
        with tempfile.NamedTemporaryFile(suffix='.json.gz', delete=False) as f:
            test_data = {'test': 'data'}
            compressed = gzip.compress(json.dumps(test_data).encode('utf-8'))
            f.write(compressed)
            test_file = Path(f.name)
            
        # Create metadata file
        metadata = {
            'sha256': hashlib.sha256(compressed).hexdigest(),
            'compression_ratio': 2.5
        }
        
        meta_file = test_file.with_suffix(test_file.suffix + '.meta')
        with open(meta_file, 'w') as f:
            json.dump(metadata, f)
            
        try:
            result = validator.validate_compression_integrity(test_file)
            
            assert result.result == ValidationResult.PASS
            assert result.details['hash_match'] == True
            assert result.details['decompression_success'] == True
            
        finally:
            test_file.unlink()
            meta_file.unlink()


# Integration test helper
def create_test_chunk_file(blocks_data, tmpdir):
    """Create a test chunk file for integration testing."""
    chunker = BlockChunker(data_dir=tmpdir)
    
    chunk_data = {
        'version': '1.0',
        'chain': 'ethereum',
        'start_block': blocks_data[0]['number'],
        'end_block': blocks_data[-1]['number'],
        'created_at': datetime.utcnow().isoformat(),
        'blocks': blocks_data,
        'metadata': {
            'block_count': len(blocks_data),
            'start_timestamp': blocks_data[0]['timestamp'],
            'end_timestamp': blocks_data[-1]['timestamp'],
            'total_transactions': sum(len(b.get('transactions', [])) for b in blocks_data),
            'total_gas_used': sum(b.get('gasUsed', 0) for b in blocks_data)
        }
    }
    
    return chunker.save_chunk(chunk_data)


@pytest.mark.integration
class TestIntegration:
    """Integration tests that require actual blockchain connection."""
    
    def test_end_to_end_processing(self):
        """Test the complete pipeline if blockchain is available."""
        # This would require actual blockchain connection
        # Skip if not available
        pytest.skip("Requires live blockchain connection")
        
    def test_chunk_creation_and_validation(self):
        """Test creating and validating chunks."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create mock block data
            blocks_data = [
                {
                    'number': 100,
                    'hash': '0xabc123',
                    'parentHash': '0xdef456',
                    'timestamp': 1640995200,
                    'miner': '0xminer',
                    'difficulty': 1000,
                    'size': 1024,
                    'gasUsed': 100000,
                    'gasLimit': 300000,
                    'transactions': [
                        {
                            'hash': '0xtx1',
                            'from': '0xfrom',
                            'to': '0xto',
                            'value': '1000000000000000000',
                            'gas': 21000,
                            'nonce': 1,
                            'input': '0x',
                            'trace': {'type': 'call'},
                            'receipt': {'status': 1}
                        }
                    ]
                },
                {
                    'number': 101,
                    'hash': '0xdef456',
                    'parentHash': '0xabc123',
                    'timestamp': 1640995212,
                    'miner': '0xminer',
                    'difficulty': 1000,
                    'size': 512,
                    'gasUsed': 50000,
                    'gasLimit': 300000,
                    'transactions': []
                }
            ]
            
            # Create chunk
            filename = create_test_chunk_file(blocks_data, tmpdir)
            chunk_file = Path(tmpdir) / 'chunks' / filename
            
            # Validate chunk
            validator = ChunkValidator()
            result = validator.run_full_validation(chunk_file)
            
            assert result['success'] == True
            assert result['summary']['pass'] > 0


if __name__ == '__main__':
    # Run tests
    pytest.main([__file__, '-v', '--tb=short'])