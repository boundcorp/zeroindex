# ZeroIndex - Blockchain Data Processing System

## Project Overview
A Django-based system for managing blockchain nodes in Kubernetes and processing blockchain data into indexed chunks.

## Key Learnings & Best Practices

### Package Management
- Install packages via `pyproject.toml` then run `make deps`
- Use `uv` for Python package management in the virtualenv

### Secret Management
- Never write secrets into flatfiles
- Install secrets into cluster with `kubectl create secret`
- Mount secrets onto pods via volume mounts
- Use `.env.local` for development secrets (not committed)

### Node Management
- This project has a `nodes` app for managing blockchain nodes in Kubernetes
- Always use app tools for node management, not direct kubectl commands
- Update templates in the app code, then run app tools to apply changes
- If tools don't exist, create them - this is designed to be a comprehensive toolset

### Ethereum Node Sync Phases
1. **Chain Download**: Initial block header sync
2. **State Healing**: Critical phase where node rebuilds state trie
3. **Post-Healing Phases** (run concurrently):
   - Snapshot generation
   - Transaction indexing  
   - Log indexing
4. **Fully Synced**: All phases complete

### Monitoring Scripts
- `scripts/advanced_eth_monitor.py`: Comprehensive monitoring handling all sync phases
- Detects and displays concurrent post-healing processes
- Shows progress bars and ETAs for each phase

### Chunk Data Collection
- **Chunk Model**: Tracks daily blockchain data segments
- **Key Fields**: `chunk_date` (not `date`), `start_block`, `end_block`
- **Management Command**: `collect_chunk_data` for fetching block data from RPC

### Web3 JSON Serialization
- Web3.py returns `HexBytes` objects that aren't JSON serializable
- Must convert using `.hex()` method or custom serializer:
```python
def to_json_serializable(obj):
    if hasattr(obj, 'hex'):
        return obj.hex()
    elif isinstance(obj, int):
        return obj
    elif obj is None:
        return None
    else:
        return str(obj)
```

### Cluster Networking
- Use cluster service names for internal communication
- Example: `http://10.43.71.202:8545` for Geth RPC
- No port forwarding needed within cluster
- Consensus API: port 5052, Execution RPC: port 8545

### Performance Considerations
- Ethereum state healing requires high IOPS (1000+)
- NFS + HDD storage causes severe bottlenecks (~8 IOPS)
- Local SSD storage recommended for blockchain nodes
- Chunk collection processes ~2-3 blocks/second on standard setup

### Database Configuration
- PostgreSQL in cluster: `postgres-primary.database.svc`
- Database credentials from Kubernetes secrets
- ArrayField not compatible with SQLite (use PostgreSQL for development)

### Common Issues & Solutions
1. **JWT Setup Pod Loop**: EmptyDir volumes don't share between pods
   - Solution: Delete unnecessary JWT setup jobs if Engine API already working
2. **Consensus Client Crashes**: Often due to execution client state changes
   - "beacon syncer reorging" errors are normal during sync
3. **Transaction Indexing**: Causes "optimistic head" warnings in consensus client
   - This is normal and resolves when indexing completes

### Development Workflow
1. Check node sync status with monitoring scripts
2. Create chunks for historical data processing
3. Verify 100% data completeness before processing
4. Use management commands for bulk operations