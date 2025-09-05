#!/bin/bash
# Quick sync status check for Ethereum node

cd /home/dev/p/boundcorp/zeroindex
export $(cat .env.local | grep -v '^#' | xargs)

python manage.py shell -c "
from zeroindex.apps.nodes.models import Node
from zeroindex.apps.chains.models import Chain

TARGET_BLOCK = 23242692

eth_chain = Chain.objects.get(chain_id=1)
node = Node.objects.filter(chain=eth_chain).first()

if node:
    print('🔗 ETHEREUM NODE SYNC STATUS')
    print('=' * 40)
    print(f'Node: {node.name}')
    print(f'Status: {node.status}')
    print(f'Execution Sync: {node.execution_sync_progress:.2f}%')
    print(f'Consensus Sync: {node.consensus_sync_progress:.2f}%')
    print(f'Current Block: {node.current_block_height:,}')
    print(f'Target Block: {TARGET_BLOCK:,}')
    if node.current_block_height:
        blocks_behind = TARGET_BLOCK - node.current_block_height
        if blocks_behind > 0:
            print(f'Blocks Behind: {blocks_behind:,}')
        else:
            print('✅ SYNCED PAST TARGET!')
    print('=' * 40)
else:
    print('❌ No Ethereum node found')
" 2>/dev/null | grep -v "imported automatically"