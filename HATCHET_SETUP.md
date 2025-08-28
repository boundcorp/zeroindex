# Hatchet Self-Hosted Setup

This project is configured to use a self-hosted Hatchet instance for workflow processing. Follow these steps to get it running.

## Prerequisites

1. **Hatchet Server**: You need to run the Hatchet server on your host machine
2. **PostgreSQL**: Hatchet requires PostgreSQL (same instance as your Django app is fine)
3. **Redis**: Optional but recommended for better performance

## Quick Setup

### 1. Install and Run Hatchet Server

```bash
# Install Hatchet CLI (if not already installed)
curl -L https://app.onhatchet.run/api/v1/cli/releases/latest/download | bash

# Initialize Hatchet in a separate directory
mkdir ~/hatchet-server && cd ~/hatchet-server
hatchet init

# Start the Hatchet server (runs on http://localhost:8080 by default)
hatchet server start
```

### 2. Configure Your Environment

Copy the environment file and configure it:

```bash
cp .env.local.example .env.local
```

Edit `.env.local` and set:

```bash
# Hatchet configuration
HATCHET_SERVER_URL=http://localhost:8080
HATCHET_CLIENT_TOKEN=your-token-here
HATCHET_NAMESPACE=default
```

### 3. Get Your Client Token

1. Open http://localhost:8080 in your browser
2. Go to the admin interface
3. Create a new client token
4. Copy the token to your `.env.local` file

### 4. Test Configuration

```bash
source .venv/bin/activate
./manage.py hatchet_status
```

This command will verify your Hatchet configuration and show you the current settings.

### 5. Start the Worker

```bash
source .venv/bin/activate
./manage.py hatchet_worker --worker-name django-worker
```

## Usage Examples

### Trigger Workflows from Django

```python
from zeroindex.utils.workflows import WorkflowTrigger

# Welcome a new user
WorkflowTrigger.welcome_new_user("user@example.com", user_id=123)

# Process some data
WorkflowTrigger.process_data("csv", {"rows": 1000})
```

### Example Workflows

The project includes two example workflows:

1. **WelcomeUserWorkflow** (`user:welcome` event)
   - Prepares welcome message
   - Sends welcome email (simulated)
   - Updates user status

2. **DataProcessingWorkflow** (`data:process` event)
   - Validates incoming data
   - Processes the data
   - Returns results

## Troubleshooting

### Common Issues

1. **Connection refused**: Make sure Hatchet server is running on port 8080
2. **Authentication failed**: Verify your client token is correct
3. **Workflows not triggering**: Check the worker logs for errors

### Debug Commands

```bash
# Check configuration
./manage.py hatchet_status

# Run worker with verbose logging
DEBUG=true ./manage.py hatchet_worker --worker-name debug-worker
```

### Logs

- **Hatchet server logs**: Check your `~/hatchet-server` directory
- **Worker logs**: Will appear in your Django app console
- **Workflow logs**: Available in the Hatchet web UI at http://localhost:8080

## Production Considerations

For production deployment:

1. Use HTTPS for Hatchet server (`HATCHET_SERVER_URL=https://your-domain.com`)
2. Set up proper authentication and authorization
3. Use a dedicated PostgreSQL database for Hatchet
4. Consider running multiple workers for high availability
5. Set up monitoring and alerting for failed workflows

## Development Workflow

1. Start Hatchet server: `cd ~/hatchet-server && hatchet server start`
2. Start Django app: `source .venv/bin/activate && ./manage.py runserver`
3. Start worker: `source .venv/bin/activate && ./manage.py hatchet_worker`
4. Trigger workflows from your Django app or via the Hatchet UI

Now you have a complete self-hosted workflow processing system!