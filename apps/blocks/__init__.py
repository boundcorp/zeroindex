"""
Blocks App - Ethereum blockchain data processing suite.

Designed for efficient extraction, compression, and storage of blockchain data
in 10-minute chunks. Built for cloud storage and optimized for analysis.

Modules:
- trace_verifier: Verify node trace capabilities
- block_chunker: Create compressed block chunks  
- s3_uploader: Upload chunks to S3
- test_chunks: Validate chunk integrity
- cli: Command line interface

Usage:
    python -m apps.blocks.cli --help
"""

__version__ = "1.0.0"
__author__ = "Leeward (coding from 30,000 feet)"

from .trace_verifier import TraceVerifier
from .block_chunker import BlockChunker
from .s3_uploader import S3Uploader
from .test_chunks import ChunkValidator

__all__ = [
    'TraceVerifier',
    'BlockChunker', 
    'S3Uploader',
    'ChunkValidator'
]