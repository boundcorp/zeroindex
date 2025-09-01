#!/bin/bash

# Ethereum node sync status monitor
# Updates every 20 minutes with formatted progress

while true; do
    clear
    echo "========================================="
    echo "  ETHEREUM NODE SYNC STATUS"
    echo "  $(date '+%Y-%m-%d %H:%M:%S')"
    echo "========================================="
    echo ""
    
    # Get latest logs
    LOGS=$(kubectl logs eth-mainnet-01-execution-856548fb84-4swhk -n devbox --tail=10 2>/dev/null | grep -E "(Syncing|chain|state)" | tail -2)
    
    if [ -z "$LOGS" ]; then
        echo "❌ Unable to fetch node status"
    else
        # Parse chain sync line
        CHAIN_LINE=$(echo "$LOGS" | grep "chain download" | tail -1)
        if [ ! -z "$CHAIN_LINE" ]; then
            CHAIN_PERCENT=$(echo "$CHAIN_LINE" | grep -oP 'synced=\K[0-9.]+')
            CHAIN_SIZE=$(echo "$CHAIN_LINE" | grep -oP 'chain=\K[0-9.]+[A-Za-z]+')
            CHAIN_BLOCKS=$(echo "$CHAIN_LINE" | grep -oP 'headers=\K[0-9,]+')
            CHAIN_ETA=$(echo "$CHAIN_LINE" | grep -oP 'eta=\K[^s]+')
            
            echo "📊 CHAIN SYNC"
            echo "  Progress: ${CHAIN_PERCENT}%"
            echo "  Blocks:   ${CHAIN_BLOCKS}"
            echo "  Size:     ${CHAIN_SIZE}"
            echo "  ETA:      ${CHAIN_ETA}"
            echo ""
        fi
        
        # Parse state sync line
        STATE_LINE=$(echo "$LOGS" | grep "state download" | tail -1)
        if [ ! -z "$STATE_LINE" ]; then
            STATE_PERCENT=$(echo "$STATE_LINE" | grep -oP 'synced=\K[0-9.]+')
            STATE_SIZE=$(echo "$STATE_LINE" | grep -oP 'state=\K[0-9.]+[A-Za-z]+')
            STATE_ACCOUNTS=$(echo "$STATE_LINE" | grep -oP 'accounts=\K[0-9,]+')
            STATE_ETA=$(echo "$STATE_LINE" | grep -oP 'eta=\K[^s]+')
            
            echo "💾 STATE SYNC"
            echo "  Progress: ${STATE_PERCENT}%"
            echo "  Accounts: ${STATE_ACCOUNTS}"
            echo "  Size:     ${STATE_SIZE}"
            echo "  ETA:      ${STATE_ETA}"
            echo ""
        fi
        
        # Progress bar for chain sync
        if [ ! -z "$CHAIN_PERCENT" ]; then
            FILLED=$(printf "%.0f" $(echo "$CHAIN_PERCENT * 0.2" | bc -l))
            EMPTY=$(echo "20 - $FILLED" | bc)
            echo -n "  Chain:  ["
            printf '█%.0s' $(seq 1 $FILLED 2>/dev/null)
            printf '░%.0s' $(seq 1 $EMPTY 2>/dev/null)
            echo "] ${CHAIN_PERCENT}%"
        fi
        
        # Progress bar for state sync
        if [ ! -z "$STATE_PERCENT" ]; then
            FILLED=$(printf "%.0f" $(echo "$STATE_PERCENT * 0.2" | bc -l))
            EMPTY=$(echo "20 - $FILLED" | bc)
            echo -n "  State:  ["
            printf '█%.0s' $(seq 1 $FILLED 2>/dev/null)
            printf '░%.0s' $(seq 1 $EMPTY 2>/dev/null)
            echo "] ${STATE_PERCENT}%"
        fi
    fi
    
    echo ""
    echo "========================================="
    echo "  Next update in 20 minutes..."
    echo "  Press Ctrl+C to exit"
    echo "========================================="
    
    # Sleep for 20 minutes
    sleep 1200
done