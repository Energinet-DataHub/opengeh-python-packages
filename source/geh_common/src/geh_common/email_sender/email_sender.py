from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

from .settings.email_sender_settings import EmailSenderSettings


def send_email(subject: str, plain_text_content: str) -> None:
    settings = EmailSenderSettings()
    message = Mail(
        from_email=settings.email_from,
        to_emails=settings.email_to,
        subject=subject,
        plain_text_content=plain_text_content,
    )

    SendGridAPIClient(settings.sendgrid_api_key.get_secret_value()).send(message)
