"""
Utilities for working with Hatchet workflows in Django.
"""
import logging
from typing import Any, Dict, Optional
from django.conf import settings
from zeroindex.utils.hatchet import get_hatchet_client, is_hatchet_configured

logger = logging.getLogger(__name__)


class WorkflowTrigger:
    """
    Helper class for triggering Hatchet workflows from Django.
    """

    @staticmethod
    def trigger_event(event_name: str, data: Dict[str, Any]) -> bool:
        """
        Trigger a Hatchet event/workflow.
        
        Args:
            event_name: Name of the event to trigger
            data: Data to pass to the workflow
            
        Returns:
            bool: True if event was triggered successfully, False otherwise
        """
        if not is_hatchet_configured():
            logger.warning("Hatchet is not configured, skipping workflow trigger")
            return False

        try:
            hatchet = get_hatchet_client()
            hatchet.event.push(event_name, data)
            logger.info(f"Successfully triggered event: {event_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to trigger event {event_name}: {e}")
            return False

    @staticmethod
    def welcome_new_user(user_email: str, user_id: Optional[int] = None) -> bool:
        """
        Trigger welcome workflow for a new user.
        
        Args:
            user_email: Email of the new user
            user_id: Optional user ID
            
        Returns:
            bool: True if workflow was triggered successfully
        """
        data = {
            "email": user_email,
            "user_id": user_id,
            "source": "django_app"
        }
        
        return WorkflowTrigger.trigger_event("user:welcome", data)

    @staticmethod
    def process_data(data_type: str, content: Any, metadata: Optional[Dict] = None) -> bool:
        """
        Trigger data processing workflow.
        
        Args:
            data_type: Type of data to process
            content: The actual data content
            metadata: Optional metadata about the processing request
            
        Returns:
            bool: True if workflow was triggered successfully
        """
        data = {
            "type": data_type,
            "content": content,
            "metadata": metadata or {},
            "source": "django_app"
        }
        
        return WorkflowTrigger.trigger_event("data:process", data)


# Django signal handlers (optional - you can connect these to Django signals)
def handle_user_created(sender, instance, created, **kwargs):
    """
    Signal handler for when a new user is created.
    
    Usage:
        from django.db.models.signals import post_save
        from django.contrib.auth.models import User
        post_save.connect(handle_user_created, sender=User)
    """
    if created:
        WorkflowTrigger.welcome_new_user(
            user_email=instance.email,
            user_id=instance.pk
        )