from .email_sender import send_email
from .settings.email_sender_settings import EmailSenderSettings

__all__ = [
    "EmailSenderSettings",
    "send_email",
]
