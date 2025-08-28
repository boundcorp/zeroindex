"""
Example Hatchet workflows for demonstration.
"""
import logging
from typing import Dict, Any
from hatchet_sdk import step
from zeroindex.utils.hatchet import get_hatchet_client

logger = logging.getLogger(__name__)

# Get Hatchet client
hatchet = get_hatchet_client()


@hatchet.workflow(on_events=["user:welcome"])
class WelcomeUserWorkflow:
    """
    Example workflow that sends a welcome message to a new user.
    
    This workflow is triggered by the "user:welcome" event and demonstrates
    basic step chaining and data passing between steps.
    """

    @step()
    def prepare_welcome_message(self, context) -> Dict[str, Any]:
        """
        First step: prepare the welcome message data.
        """
        user_data = context["workflow"]["input"]
        user_email = user_data.get("email", "Unknown User")
        
        logger.info(f"Preparing welcome message for user: {user_email}")
        
        return {
            "email": user_email,
            "message": f"Welcome to ZeroIndex, {user_email}!",
            "template": "user_welcome"
        }

    @step(parents=["prepare_welcome_message"])
    def send_welcome_email(self, context) -> Dict[str, Any]:
        """
        Second step: send the welcome email.
        """
        message_data = context["parents"]["prepare_welcome_message"]
        
        # Here you would integrate with your email service
        # For now, we'll just log it
        logger.info(f"Sending welcome email to {message_data['email']}")
        logger.info(f"Message: {message_data['message']}")
        
        # Simulate email sending
        return {
            "email_sent": True,
            "sent_at": context["workflow"]["started_at"],
            "recipient": message_data["email"]
        }

    @step(parents=["send_welcome_email"])
    def update_user_status(self, context) -> Dict[str, Any]:
        """
        Final step: update user status to indicate welcome flow is complete.
        """
        email_result = context["parents"]["send_welcome_email"]
        
        if email_result["email_sent"]:
            logger.info(f"Welcome flow completed for {email_result['recipient']}")
            
            # Here you would update the user record in your database
            # For now, we'll just return success
            return {
                "welcome_flow_completed": True,
                "completed_at": context["workflow"]["started_at"]
            }
        else:
            logger.error(f"Failed to send welcome email to {email_result['recipient']}")
            return {
                "welcome_flow_completed": False,
                "error": "Email sending failed"
            }


@hatchet.workflow(on_events=["data:process"])
class DataProcessingWorkflow:
    """
    Example workflow for background data processing.
    
    This workflow demonstrates how you might process data asynchronously.
    """

    @step(timeout="5m")
    def validate_data(self, context) -> Dict[str, Any]:
        """
        Validate the incoming data.
        """
        data = context["workflow"]["input"]
        
        logger.info("Validating data...")
        
        # Basic validation example
        required_fields = ["type", "content"]
        missing_fields = [field for field in required_fields if field not in data]
        
        if missing_fields:
            return {
                "valid": False,
                "errors": f"Missing required fields: {', '.join(missing_fields)}"
            }
        
        return {
            "valid": True,
            "data": data
        }

    @step(parents=["validate_data"], timeout="10m")
    def process_data(self, context) -> Dict[str, Any]:
        """
        Process the validated data.
        """
        validation_result = context["parents"]["validate_data"]
        
        if not validation_result["valid"]:
            logger.error(f"Data validation failed: {validation_result['errors']}")
            return {
                "processed": False,
                "error": validation_result["errors"]
            }
        
        data = validation_result["data"]
        logger.info(f"Processing data of type: {data['type']}")
        
        # Simulate some processing work
        import time
        time.sleep(2)  # Simulate processing time
        
        return {
            "processed": True,
            "result": f"Processed {data['type']} data successfully",
            "processed_at": context["workflow"]["started_at"]
        }


# Example function to trigger workflows programmatically
def trigger_welcome_workflow(user_email: str):
    """
    Helper function to trigger the welcome workflow for a new user.
    
    Args:
        user_email: Email of the user to welcome
    """
    try:
        hatchet = get_hatchet_client()
        hatchet.event.push(
            "user:welcome",
            {
                "email": user_email,
                "timestamp": "2023-01-01T00:00:00Z"  # You'd use actual timestamp
            }
        )
        logger.info(f"Triggered welcome workflow for {user_email}")
    except Exception as e:
        logger.error(f"Failed to trigger welcome workflow for {user_email}: {e}")


def trigger_data_processing_workflow(data_type: str, content: Any):
    """
    Helper function to trigger data processing workflow.
    
    Args:
        data_type: Type of data to process
        content: The actual data content
    """
    try:
        hatchet = get_hatchet_client()
        hatchet.event.push(
            "data:process",
            {
                "type": data_type,
                "content": content
            }
        )
        logger.info(f"Triggered data processing workflow for {data_type}")
    except Exception as e:
        logger.error(f"Failed to trigger data processing workflow: {e}")