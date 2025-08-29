#!/usr/bin/env python3
"""
Chain configuration for block processing.
Per-chain settings for chunk duration and block timing.
"""

from dataclasses import dataclass
from typing import Dict, Optional


@dataclass
class ChainConfig:
    """Configuration for a blockchain."""
    
    chain_id: int
    name: str
    symbol: str = "ETH"
    chunk_duration_days: int = 1
    estimated_blocks_per_day: int = 7200
    average_block_time_seconds: float = 12.0
    default_rpc_url: Optional[str] = None
    
    @property
    def blocks_per_chunk(self) -> int:
        """Calculate blocks per chunk based on duration and daily blocks."""
        return self.estimated_blocks_per_day * self.chunk_duration_days
        
    @property
    def seconds_per_chunk(self) -> int:
        """Calculate seconds per chunk."""
        return int(self.blocks_per_chunk * self.average_block_time_seconds)
        
    @property
    def chunk_duration_hours(self) -> float:
        """Get chunk duration in hours."""
        return self.chunk_duration_days * 24
        

# Built-in chain configurations
CHAIN_CONFIGS: Dict[int, ChainConfig] = {
    1: ChainConfig(
        chain_id=1,
        name="Ethereum Mainnet",
        symbol="ETH",
        chunk_duration_days=1,
        estimated_blocks_per_day=7200,  # ~12 seconds per block
        average_block_time_seconds=12.0,
        default_rpc_url="https://ethereum.publicnode.com"
    ),
    137: ChainConfig(
        chain_id=137,
        name="Polygon",
        symbol="MATIC", 
        chunk_duration_days=1,
        estimated_blocks_per_day=43200,  # ~2 seconds per block
        average_block_time_seconds=2.0,
        default_rpc_url="https://polygon.publicnode.com"
    ),
    56: ChainConfig(
        chain_id=56,
        name="BNB Smart Chain",
        symbol="BNB",
        chunk_duration_days=1,
        estimated_blocks_per_day=28800,  # ~3 seconds per block
        average_block_time_seconds=3.0,
        default_rpc_url="https://bsc.publicnode.com"
    ),
    42161: ChainConfig(
        chain_id=42161,
        name="Arbitrum One",
        symbol="ETH",
        chunk_duration_days=1,
        estimated_blocks_per_day=216000,  # ~0.4 seconds per block
        average_block_time_seconds=0.4,
        default_rpc_url="https://arbitrum.publicnode.com"
    ),
    10: ChainConfig(
        chain_id=10,
        name="Optimism",
        symbol="ETH", 
        chunk_duration_days=1,
        estimated_blocks_per_day=43200,  # ~2 seconds per block
        average_block_time_seconds=2.0,
        default_rpc_url="https://optimism.publicnode.com"
    ),
    8453: ChainConfig(
        chain_id=8453,
        name="Base",
        symbol="ETH",
        chunk_duration_days=1,
        estimated_blocks_per_day=43200,  # ~2 seconds per block  
        average_block_time_seconds=2.0,
        default_rpc_url="https://base.publicnode.com"
    ),
    5: ChainConfig(
        chain_id=5,
        name="Ethereum Goerli Testnet",
        symbol="GoerliETH",
        chunk_duration_days=1,
        estimated_blocks_per_day=7200,  # ~12 seconds per block
        average_block_time_seconds=12.0,
        default_rpc_url="https://ethereum-goerli.publicnode.com"
    )
}


def get_chain_config(chain_id: int) -> ChainConfig:
    """Get configuration for a specific chain."""
    return CHAIN_CONFIGS.get(chain_id, CHAIN_CONFIGS[1])  # Default to Ethereum


def get_supported_chains() -> Dict[int, ChainConfig]:
    """Get all supported chain configurations."""
    return CHAIN_CONFIGS.copy()


def estimate_chunk_size_mb(chain_id: int, avg_tx_per_block: int = 200) -> float:
    """Estimate compressed chunk size in MB for a chain."""
    config = get_chain_config(chain_id)
    
    # Base estimates from our Ethereum testing:
    # - 50 blocks with 272 avg txs = 2.56 MB compressed
    # - So ~0.051 MB per block, ~0.000187 MB per transaction
    
    mb_per_tx = 0.000187  # From our real data
    total_txs = config.blocks_per_chunk * avg_tx_per_block
    estimated_mb = total_txs * mb_per_tx
    
    return estimated_mb


# Example usage and testing
if __name__ == "__main__":
    print("📊 Supported Chain Configurations:")
    print("=" * 70)
    
    for chain_id, config in CHAIN_CONFIGS.items():
        print(f"\n🔗 {config.name} (Chain ID: {chain_id})")
        print(f"   Symbol: {config.symbol}")
        print(f"   Chunk duration: {config.chunk_duration_days} day(s)")
        print(f"   Blocks per chunk: {config.blocks_per_chunk:,}")
        print(f"   Est. chunk duration: {config.chunk_duration_hours:.1f} hours")
        print(f"   Block time: {config.average_block_time_seconds} seconds")
        print(f"   Est. chunk size: {estimate_chunk_size_mb(chain_id):.2f} MB")
        print(f"   RPC: {config.default_rpc_url}")
    
    print(f"\n🎯 Ethereum (Chain ID 1) Details:")
    eth_config = get_chain_config(1)
    print(f"   1-day chunk = {eth_config.blocks_per_chunk:,} blocks")
    print(f"   Should take ~{eth_config.seconds_per_chunk:,} seconds ({eth_config.seconds_per_chunk/3600:.1f} hours)")
    print(f"   Est. size with 200 tx/block: {estimate_chunk_size_mb(1, 200):.2f} MB")
    print(f"   Est. size with 300 tx/block: {estimate_chunk_size_mb(1, 300):.2f} MB")