"""
Hatchet configuration and utilities for zeroindex project.
Self-hosted Hatchet setup.
"""
import os
from typing import Optional
from hatchet_sdk import Hatchet, ClientConfig

# Initialize Hatchet client as a global variable
_hatchet_client: Optional[Hatchet] = None


def get_hatchet_client() -> Hatchet:
    """
    Get the configured Hatchet client instance for self-hosted setup.
    
    Returns:
        Hatchet: The configured Hatchet client
        
    Raises:
        ValueError: If required environment variables are not set
    """
    global _hatchet_client
    
    if _hatchet_client is None:
        # Self-hosted configuration
        server_url = os.getenv("HATCHET_SERVER_URL", "http://localhost:8080")
        token = os.getenv("HATCHET_CLIENT_TOKEN", "")
        
        # For Hatchet Lite, we might not need a token
        # Create client config for self-hosted instance
        if token:
            config = ClientConfig(
                server_url=server_url,
                token=token,
                # Additional self-hosted options
                tls_config=None if server_url.startswith("http://") else None,
                namespace=os.getenv("HATCHET_NAMESPACE", "default")
            )
        else:
            # Lite version without token
            config = ClientConfig(
                server_url=server_url,
                # No token for lite version
                tls_config=None,
                namespace=os.getenv("HATCHET_NAMESPACE", "default")
            )
        
        # Create Hatchet client with self-hosted configuration
        _hatchet_client = Hatchet(
            debug=os.getenv("DEBUG", "").lower() == "true",
            config=config
        )
    
    return _hatchet_client


def is_hatchet_configured() -> bool:
    """
    Check if Hatchet is properly configured for self-hosting.
    
    Returns:
        bool: True if Hatchet is configured, False otherwise
    """
    return bool(os.getenv("HATCHET_CLIENT_TOKEN")) and bool(os.getenv("HATCHET_SERVER_URL"))


def get_hatchet_config() -> dict:
    """
    Get current Hatchet configuration for debugging.
    
    Returns:
        dict: Current configuration values
    """
    return {
        "server_url": os.getenv("HATCHET_SERVER_URL", "http://localhost:8080"),
        "namespace": os.getenv("HATCHET_NAMESPACE", "default"),
        "token_configured": bool(os.getenv("HATCHET_CLIENT_TOKEN")),
        "debug": os.getenv("DEBUG", "").lower() == "true"
    }