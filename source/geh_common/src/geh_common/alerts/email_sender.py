from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

from .settings.alert_email_settings import AlertEmailSettings


def send_comparison_alert(subject: str, plain_text_content: str) -> None:
    settings = AlertEmailSettings()
    message = Mail(
        from_email=str(settings.alert_email_from),
        to_emails=str(settings.alert_email_to),
        subject=subject,
        plain_text_content=plain_text_content,
    )

    SendGridAPIClient(settings.sendgrid_api_key.get_secret_value()).send(message)
