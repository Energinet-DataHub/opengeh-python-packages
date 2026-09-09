from unittest.mock import MagicMock, patch

from pytest import MonkeyPatch

from geh_common.email_sender import send_email


@patch("geh_common.email_sender.email_sender.Mail")
@patch("geh_common.email_sender.email_sender.SendGridAPIClient")
def test_send_email__sends_plain_text_email_using_environment_settings(
    sendgrid_client: MagicMock,
    mail: MagicMock,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setenv("SENDGRID_API_KEY", "api-key")
    monkeypatch.setenv("ALERT_EMAIL_FROM", "sender@example.com")
    monkeypatch.setenv("ALERT_EMAIL_TO", "recipient@example.com")
    message = mail.return_value

    send_email("Comparison failed", "2 records are missing")

    mail.assert_called_once_with(
        from_email="sender@example.com",
        to_emails="recipient@example.com",
        subject="Comparison failed",
        plain_text_content="2 records are missing",
    )
    sendgrid_client.assert_called_once_with("api-key")
    sendgrid_client.return_value.send.assert_called_once_with(message)
