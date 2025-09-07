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

### CRITICAL: Blockchain Data Protection
- **NEVER delete blockchain node PVCs without explicit user permission**
- Ethereum full sync takes DAYS/WEEKS - sync data is irreplaceable
- Always check for existing data volumes before making changes
- If PVC issues occur, investigate and ask user before any destructive actions
- Backup/migration strategies must be discussed with user first

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

### Production Ethereum Node Deployment

#### Initial Setup
```bash
# Set up HOME cluster credentials
python manage.py setup_home_cluster --namespace devbox

# Create and start a node
python manage.py create_node eth-mainnet-01 --start
```

#### Resource Requirements (CRITICAL)
**Execution Client (Geth):**
- Memory: 16Gi limit, 8Gi request
- CPU: 4 cores limit, 2 cores request
- Storage: 2TB minimum (8TB recommended for growth)

**Consensus Client (Lighthouse):**
- Memory: **12Gi limit, 6Gi request** (8Gi causes OOM during sync)
- CPU: 4 cores limit, 2 cores request  
- Storage: 200GB minimum

#### Node Selection
- **Preferred nodes**: vega, nova (have working NFS CSI drivers)
- **Avoid**: enterprise, ziti (resource constrained, cause pod failures)
- Use `kubernetes.io/hostname` selector for explicit placement

#### Storage Classes
- HOME cluster: `nfs-iota-hdd-slush` (NFS-based, good for blockchain data)
- Avoid `local-path` for production (node-specific, not portable)

#### Common Issues & Fixes
1. **Consensus OOM kills (exit code 137)**
   - Increase memory limit to 12Gi minimum
   - Watch for "Database write failed" errors in logs

2. **Pod stuck in Init phase**
   - Check PVC mounting issues
   - Verify NFS CSI driver is running on target node
   - May need to restart k3s-agent on problematic nodes

3. **Database lock errors**
   - Scale deployment to 0, then back to 1
   - Ensures clean shutdown and lock release

4. **Nova node issues**
   - New nodes may have Cilium/CSI initialization problems
   - SSH to node and restart k3s-agent if needed
   - Check for "services have not yet been read" errors