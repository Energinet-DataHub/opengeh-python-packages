# Email sender

Use `send_email` to send a plain-text email through SendGrid:

```python
from geh_common.email_sender import send_email

send_email(
    subject="Wholesale comparison failed",
    plain_text_content="2 calculated records are missing from published results",
)
```

The sender reads these environment variables:

- `SENDGRID_API_KEY`: SendGrid API key.
- `ALERT_EMAIL_FROM`: Sender email address.
- `ALERT_EMAIL_TO`: Recipient email address.

`EmailSenderSettings` is also exported from `geh_common.email_sender` when the
resolved configuration is needed directly:

```python
from geh_common.email_sender import EmailSenderSettings

settings = EmailSenderSettings()
```
