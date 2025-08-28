"""
Django management command to fetch the latest Base chain block information.
"""
import os
import json
from datetime import datetime
from django.core.management.base import BaseCommand
import requests


class Command(BaseCommand):
    help = 'Fetch latest Base chain block information including timestamp and transaction count'

    def add_arguments(self, parser):
        parser.add_argument(
            '--tx',
            type=str,
            help='Transaction hash to inspect for ETH transfers'
        )
        parser.add_argument(
            '--tx-index',
            type=int,
            help='Transaction index in the latest block to inspect'
        )

    def get_eth_transfers_from_tx(self, rpc_url, tx_hash):
        """Get all ETH transfers from a transaction using debug_traceTransaction."""
        transfers = []
        
        # Try debug_traceTransaction with callTracer
        trace_payload = {
            "jsonrpc": "2.0",
            "method": "debug_traceTransaction",
            "params": [
                tx_hash,
                {"tracer": "callTracer"}
            ],
            "id": 3
        }
        
        try:
            response = requests.post(rpc_url, json=trace_payload)
            response.raise_for_status()
            result = response.json()
            
            if 'error' in result:
                # Fallback to trying trace_transaction (Parity/OpenEthereum style)
                trace_payload = {
                    "jsonrpc": "2.0",
                    "method": "trace_transaction",
                    "params": [tx_hash],
                    "id": 4
                }
                response = requests.post(rpc_url, json=trace_payload)
                response.raise_for_status()
                result = response.json()
                
                if 'error' in result:
                    return None, result['error']['message']
                
                # Parse Parity-style traces
                for trace in result.get('result', []):
                    if trace['type'] == 'call' and trace['action']['callType'] in ['call', 'transfer']:
                        if int(trace['action'].get('value', '0x0'), 16) > 0:
                            transfers.append({
                                'from': trace['action']['from'],
                                'to': trace['action']['to'],
                                'value_wei': int(trace['action']['value'], 16),
                                'value_eth': int(trace['action']['value'], 16) / 10**18,
                                'type': trace['action']['callType']
                            })
            else:
                # Parse debug_traceTransaction result
                def extract_transfers(call_obj, transfers_list):
                    if call_obj.get('value') and int(call_obj['value'], 16) > 0:
                        transfers_list.append({
                            'from': call_obj.get('from', 'unknown'),
                            'to': call_obj.get('to', 'unknown'),
                            'value_wei': int(call_obj['value'], 16),
                            'value_eth': int(call_obj['value'], 16) / 10**18,
                            'type': call_obj.get('type', 'unknown')
                        })
                    
                    # Recursively check internal calls
                    for internal_call in call_obj.get('calls', []):
                        extract_transfers(internal_call, transfers_list)
                
                extract_transfers(result.get('result', {}), transfers)
                
        except Exception as e:
            return None, str(e)
        
        return transfers, None

    def handle(self, *args, **options):
        rpc_url = os.environ.get('BASE_MAINNET_RPC_URL')
        
        if not rpc_url:
            self.stdout.write(
                self.style.ERROR('BASE_MAINNET_RPC_URL is not configured in environment variables')
            )
            return
        
        self.stdout.write(f'Fetching latest block from Base Mainnet...')
        self.stdout.write(f'RPC URL: {rpc_url}')
        
        try:
            # Get latest block number
            latest_block_payload = {
                "jsonrpc": "2.0",
                "method": "eth_blockNumber",
                "params": [],
                "id": 1
            }
            
            response = requests.post(rpc_url, json=latest_block_payload)
            response.raise_for_status()
            latest_block_hex = response.json()['result']
            latest_block_num = int(latest_block_hex, 16)
            
            # Get block details with transaction data
            block_details_payload = {
                "jsonrpc": "2.0",
                "method": "eth_getBlockByNumber",
                "params": [latest_block_hex, True],  # True to include full transaction objects
                "id": 2
            }
            
            response = requests.post(rpc_url, json=block_details_payload)
            response.raise_for_status()
            block_data = response.json()['result']
            
            # Extract information
            timestamp = int(block_data['timestamp'], 16)
            block_time = datetime.fromtimestamp(timestamp)
            num_transactions = len(block_data['transactions'])
            
            # Display results
            self.stdout.write('\n' + '=' * 50)
            self.stdout.write(self.style.SUCCESS('✓ Latest Base Chain Block Information'))
            self.stdout.write('=' * 50)
            self.stdout.write(f'Block Number: {latest_block_num:,}')
            self.stdout.write(f'Block Hash: {block_data["hash"]}')
            self.stdout.write(f'Timestamp: {timestamp} ({block_time.strftime("%Y-%m-%d %H:%M:%S UTC")})')
            self.stdout.write(f'Number of Transactions: {num_transactions}')
            self.stdout.write(f'Gas Used: {int(block_data["gasUsed"], 16):,}')
            self.stdout.write(f'Gas Limit: {int(block_data["gasLimit"], 16):,}')
            self.stdout.write(f'Base Fee Per Gas: {int(block_data.get("baseFeePerGas", "0x0"), 16) / 10**9:.2f} Gwei')
            self.stdout.write('=' * 50)
            
            # Handle transaction inspection if requested
            tx_hash = options.get('tx')
            tx_index = options.get('tx_index')
            
            if tx_hash:
                self.inspect_transaction(rpc_url, tx_hash)
            elif tx_index is not None:
                if tx_index < len(block_data['transactions']):
                    tx = block_data['transactions'][tx_index]
                    tx_hash = tx['hash'] if isinstance(tx, dict) else tx
                    self.stdout.write(f'\nInspecting transaction at index {tx_index}: {tx_hash}')
                    self.inspect_transaction(rpc_url, tx_hash)
                else:
                    self.stdout.write(
                        self.style.ERROR(f'\nTransaction index {tx_index} out of range (block has {num_transactions} transactions)')
                    )
            
        except requests.RequestException as e:
            self.stdout.write(
                self.style.ERROR(f'Failed to fetch block data: {e}')
            )
        except KeyError as e:
            self.stdout.write(
                self.style.ERROR(f'Unexpected response format. Missing key: {e}')
            )
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f'An error occurred: {e}')
            )
    
    def inspect_transaction(self, rpc_url, tx_hash):
        """Inspect a transaction for ETH transfers."""
        self.stdout.write('\n' + '=' * 50)
        self.stdout.write('Transaction Details')
        self.stdout.write('=' * 50)
        
        # First get basic transaction info
        tx_payload = {
            "jsonrpc": "2.0",
            "method": "eth_getTransactionByHash",
            "params": [tx_hash],
            "id": 5
        }
        
        try:
            response = requests.post(rpc_url, json=tx_payload)
            response.raise_for_status()
            tx_data = response.json().get('result')
            
            if not tx_data:
                self.stdout.write(self.style.ERROR(f'Transaction {tx_hash} not found'))
                return
            
            # Display basic transaction info
            self.stdout.write(f'Hash: {tx_hash}')
            self.stdout.write(f'From: {tx_data["from"]}')
            self.stdout.write(f'To: {tx_data.get("to", "Contract Creation")}')
            main_value = int(tx_data.get("value", "0x0"), 16) / 10**18
            self.stdout.write(f'Main Value: {main_value:.6f} ETH')
            
            # Get transaction receipt for status
            receipt_payload = {
                "jsonrpc": "2.0",
                "method": "eth_getTransactionReceipt",
                "params": [tx_hash],
                "id": 6
            }
            
            response = requests.post(rpc_url, json=receipt_payload)
            response.raise_for_status()
            receipt = response.json().get('result')
            
            if receipt:
                status = "Success" if receipt["status"] == "0x1" else "Failed"
                self.stdout.write(f'Status: {status}')
                self.stdout.write(f'Gas Used: {int(receipt["gasUsed"], 16):,}')
            
            # Try to get internal transfers
            self.stdout.write('\n--- Attempting to trace ETH transfers ---')
            transfers, error = self.get_eth_transfers_from_tx(rpc_url, tx_hash)
            
            if error:
                self.stdout.write(self.style.WARNING(f'Note: Trace methods not available on this RPC endpoint'))
                self.stdout.write(f'Error: {error}')
                self.stdout.write('\nTo see internal ETH transfers, you need an RPC provider that supports:')
                self.stdout.write('- debug_traceTransaction (Geth-style) or')
                self.stdout.write('- trace_transaction (Parity-style)')
                self.stdout.write('\nProviders with trace support: Alchemy, QuickNode (with trace addon), Infura (with trace addon)')
            elif transfers:
                self.stdout.write(f'\nFound {len(transfers)} ETH transfer(s):')
                for i, transfer in enumerate(transfers, 1):
                    self.stdout.write(f'\nTransfer #{i}:')
                    self.stdout.write(f'  From: {transfer["from"]}')
                    self.stdout.write(f'  To: {transfer["to"]}')
                    self.stdout.write(f'  Value: {transfer["value_eth"]:.6f} ETH')
                    self.stdout.write(f'  Type: {transfer["type"]}')
            else:
                self.stdout.write('No ETH transfers found in this transaction')
                
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Error inspecting transaction: {e}'))