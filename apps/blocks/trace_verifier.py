#!/usr/bin/env python3
"""
Block and transaction trace verification module.
Testing capabilities while cruising at 30,000 feet.
"""

import json
import asyncio
from typing import Dict, List, Any, Optional
from web3 import Web3, HTTPProvider
from web3.types import BlockData, TxData
import os
from datetime import datetime

class TraceVerifier:
    """Verifies our ability to retrieve full traces from Ethereum nodes."""
    
    def __init__(self, rpc_url: Optional[str] = None):
        """Initialize with RPC connection."""
        self.rpc_url = rpc_url or os.getenv('ETH_RPC_URL', 'http://ethereum-l1-node:8545')
        self.w3 = Web3(HTTPProvider(self.rpc_url))
        self.trace_methods = {
            'debug_traceBlockByNumber': self.check_debug_trace_block,
            'debug_traceTransaction': self.check_debug_trace_tx,
            'trace_block': self.check_trace_block,
            'trace_transaction': self.check_trace_transaction,
            'eth_getBlockReceipts': self.check_block_receipts
        }
        
    def check_connection(self) -> bool:
        """Verify node connection."""
        try:
            return self.w3.is_connected()
        except Exception as e:
            print(f"Connection error: {e}")
            return False
            
    def get_latest_block(self) -> Optional[int]:
        """Get latest block number."""
        try:
            return self.w3.eth.block_number
        except Exception as e:
            print(f"Error getting latest block: {e}")
            return None
            
    def check_debug_trace_block(self, block_num: int) -> Dict[str, Any]:
        """Check if debug_traceBlockByNumber is available."""
        try:
            result = self.w3.provider.make_request(
                'debug_traceBlockByNumber',
                [hex(block_num), {'tracer': 'callTracer'}]
            )
            return {
                'available': True,
                'method': 'debug_traceBlockByNumber',
                'trace_count': len(result.get('result', [])),
                'has_internal_calls': any(
                    'calls' in tx.get('result', {}) 
                    for tx in result.get('result', [])
                )
            }
        except Exception as e:
            return {
                'available': False,
                'method': 'debug_traceBlockByNumber',
                'error': str(e)
            }
            
    def check_debug_trace_tx(self, tx_hash: str) -> Dict[str, Any]:
        """Check if debug_traceTransaction is available."""
        try:
            result = self.w3.provider.make_request(
                'debug_traceTransaction',
                [tx_hash, {'tracer': 'callTracer'}]
            )
            trace = result.get('result', {})
            return {
                'available': True,
                'method': 'debug_traceTransaction',
                'has_internal_calls': 'calls' in trace,
                'call_depth': self._count_call_depth(trace)
            }
        except Exception as e:
            return {
                'available': False,
                'method': 'debug_traceTransaction',
                'error': str(e)
            }
            
    def check_trace_block(self, block_num: int) -> Dict[str, Any]:
        """Check if trace_block (Parity/OpenEthereum style) is available."""
        try:
            result = self.w3.provider.make_request(
                'trace_block',
                [hex(block_num)]
            )
            traces = result.get('result', [])
            return {
                'available': True,
                'method': 'trace_block',
                'trace_count': len(traces),
                'has_internal_transfers': any(
                    t.get('action', {}).get('callType') == 'call' 
                    for t in traces
                )
            }
        except Exception as e:
            return {
                'available': False,
                'method': 'trace_block',
                'error': str(e)
            }
            
    def check_trace_transaction(self, tx_hash: str) -> Dict[str, Any]:
        """Check if trace_transaction is available."""
        try:
            result = self.w3.provider.make_request(
                'trace_transaction',
                [tx_hash]
            )
            traces = result.get('result', [])
            return {
                'available': True,
                'method': 'trace_transaction',
                'trace_count': len(traces),
                'max_depth': max((len(t.get('traceAddress', [])) for t in traces), default=0)
            }
        except Exception as e:
            return {
                'available': False,
                'method': 'trace_transaction',
                'error': str(e)
            }
            
    def check_block_receipts(self, block_num: int) -> Dict[str, Any]:
        """Check eth_getBlockReceipts for basic event data."""
        try:
            result = self.w3.provider.make_request(
                'eth_getBlockReceipts',
                [hex(block_num)]
            )
            receipts = result.get('result', [])
            total_logs = sum(len(r.get('logs', [])) for r in receipts)
            return {
                'available': True,
                'method': 'eth_getBlockReceipts',
                'receipt_count': len(receipts),
                'total_logs': total_logs
            }
        except Exception as e:
            return {
                'available': False,
                'method': 'eth_getBlockReceipts',
                'error': str(e)
            }
            
    def _count_call_depth(self, trace: Dict, depth: int = 0) -> int:
        """Recursively count maximum call depth."""
        if 'calls' not in trace:
            return depth
        max_depth = depth
        for call in trace['calls']:
            max_depth = max(max_depth, self._count_call_depth(call, depth + 1))
        return max_depth
        
    def verify_full_trace_capability(self, sample_blocks: int = 5) -> Dict[str, Any]:
        """
        Comprehensive check of node's trace capabilities.
        Tests multiple blocks to ensure consistency.
        """
        print(f"🛫 Starting trace capability verification at {datetime.now()}")
        print(f"Testing connection to {self.rpc_url}...")
        
        if not self.check_connection():
            return {'error': 'Cannot connect to node', 'archival_required': 'unknown'}
            
        latest = self.get_latest_block()
        if not latest:
            return {'error': 'Cannot get latest block', 'archival_required': 'unknown'}
            
        print(f"Latest block: {latest}")
        
        # Test recent blocks
        results = {
            'node_info': {
                'latest_block': latest,
                'rpc_url': self.rpc_url,
                'tested_at': datetime.now().isoformat()
            },
            'capabilities': {},
            'sample_results': []
        }
        
        # Test different block ranges to check for archival data
        test_blocks = [
            latest - 1,  # Very recent
            latest - 100,  # Recent
            latest - 1000,  # Older
            latest - 10000  # Much older (may fail on non-archival)
        ]
        
        for block_num in test_blocks:
            if block_num < 0:
                continue
                
            print(f"Testing block {block_num} (latest - {latest - block_num})...")
            
            block_result = {'block': block_num, 'methods': {}}
            
            # Get block to find a transaction
            try:
                block = self.w3.eth.get_block(block_num, full_transactions=True)
                sample_tx = block['transactions'][0]['hash'].hex() if block['transactions'] else None
            except Exception as e:
                block_result['error'] = f"Cannot fetch block: {e}"
                results['sample_results'].append(block_result)
                continue
                
            # Test each trace method
            for method_name, method_func in self.trace_methods.items():
                if 'transaction' in method_name and sample_tx:
                    result = method_func(sample_tx)
                elif 'block' in method_name:
                    result = method_func(block_num)
                else:
                    continue
                    
                block_result['methods'][method_name] = result
                
                # Track overall capability
                if method_name not in results['capabilities']:
                    results['capabilities'][method_name] = result['available']
                else:
                    # If any block fails, mark as not fully available
                    results['capabilities'][method_name] = (
                        results['capabilities'][method_name] and result['available']
                    )
                    
            results['sample_results'].append(block_result)
            
        # Determine if archival node is required
        results['archival_required'] = self._determine_archival_requirement(results)
        results['recommendation'] = self._generate_recommendation(results)
        
        return results
        
    def _determine_archival_requirement(self, results: Dict) -> bool:
        """Analyze results to determine if archival node is needed."""
        # Check if older blocks failed while recent ones succeeded
        if not results.get('sample_results'):
            return True
            
        recent_success = False
        old_fail = False
        
        for sample in results['sample_results']:
            block_age = results['node_info']['latest_block'] - sample['block']
            methods = sample.get('methods', {})
            
            # Check if any trace method succeeded
            has_traces = any(
                m.get('available', False) 
                for m in methods.values()
            )
            
            if block_age < 200 and has_traces:
                recent_success = True
            elif block_age > 5000 and not has_traces:
                old_fail = True
                
        return recent_success and old_fail
        
    def _generate_recommendation(self, results: Dict) -> str:
        """Generate recommendation based on capabilities."""
        caps = results.get('capabilities', {})
        
        if caps.get('debug_traceBlockByNumber') or caps.get('debug_traceTransaction'):
            return "Full node with debug API is sufficient for complete traces"
        elif caps.get('trace_block') or caps.get('trace_transaction'):
            return "Full node with trace API is sufficient for complete traces"
        elif results.get('archival_required'):
            return "Archival node required for historical trace data"
        else:
            return "Node may not support full tracing - consider using a different client or enabling trace APIs"


async def main():
    """Main verification routine - running from the airplane wifi."""
    verifier = TraceVerifier()
    
    print("✈️ Starting trace verification from 30,000 feet...")
    print("=" * 60)
    
    results = verifier.verify_full_trace_capability(sample_blocks=3)
    
    print("\n📊 Verification Results:")
    print("=" * 60)
    print(json.dumps(results, indent=2))
    
    print("\n🎯 Summary:")
    print(f"Archival Required: {results.get('archival_required', 'Unknown')}")
    print(f"Recommendation: {results.get('recommendation', 'Unknown')}")
    
    # Save results
    output_file = f"trace_verification_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)
    print(f"\n💾 Results saved to {output_file}")
    
    return results


if __name__ == "__main__":
    asyncio.run(main())