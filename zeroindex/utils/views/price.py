from rest_framework import status
from rest_framework.decorators import api_view
from rest_framework.response import Response
from web3 import Web3
import requests


@api_view(["GET"])
def eth_price(request):
    """
    Return the current ETH price.
    First attempts to get price from chainlink oracle on mainnet,
    falls back to external API if RPC is unavailable.
    """
    try:
        # Try to get a running Ethereum mainnet node
        try:
            from zeroindex.apps.nodes.models import Node
            from zeroindex.apps.chains.models import Chain

            eth_chain = Chain.objects.filter(name__icontains='ethereum').first()
        except Exception:
            # Database not available, skip to external API
            eth_chain = None

        if eth_chain:
            node = Node.objects.filter(
                chain=eth_chain,
                status__in=['running', 'syncing'],
                execution_rpc_url__isnull=False
            ).first()

            if node and node.execution_rpc_url:
                try:
                    w3 = Web3(Web3.HTTPProvider(node.execution_rpc_url))
                    if w3.is_connected():
                        # Chainlink ETH/USD price feed address on Ethereum mainnet
                        chainlink_address = '0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419'

                        # ABI for Chainlink price feed (only latestRoundData function)
                        chainlink_abi = [
                            {
                                "inputs": [],
                                "name": "latestRoundData",
                                "outputs": [
                                    {"internalType": "uint80", "name": "roundId", "type": "uint80"},
                                    {"internalType": "int256", "name": "answer", "type": "int256"},
                                    {"internalType": "uint256", "name": "startedAt", "type": "uint256"},
                                    {"internalType": "uint256", "name": "updatedAt", "type": "uint256"},
                                    {"internalType": "uint80", "name": "answeredInRound", "type": "uint80"}
                                ],
                                "stateMutability": "view",
                                "type": "function"
                            }
                        ]

                        contract = w3.eth.contract(address=chainlink_address, abi=chainlink_abi)
                        latest_data = contract.functions.latestRoundData().call()

                        # Chainlink returns price with 8 decimals
                        price = latest_data[1] / 10**8

                        return Response({
                            "price": price,
                            "currency": "USD",
                            "source": "chainlink_oracle"
                        }, status=status.HTTP_200_OK)

                except Exception as oracle_error:
                    # Log but don't fail - we'll fall back to external API
                    pass

        # Fallback to external price API
        response = requests.get('https://api.coinbase.com/v2/exchange-rates?currency=ETH', timeout=5)
        if response.status_code == 200:
            data = response.json()
            price = float(data['data']['rates']['USD'])
            return Response({
                "price": price,
                "currency": "USD",
                "source": "coinbase_api"
            }, status=status.HTTP_200_OK)
        else:
            return Response({
                "error": "Unable to fetch ETH price from external API"
            }, status=status.HTTP_503_SERVICE_UNAVAILABLE)

    except Exception as e:
        return Response({
            "error": f"Failed to fetch ETH price: {str(e)}"
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)