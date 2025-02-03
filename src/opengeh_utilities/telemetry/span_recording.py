from opentelemetry.trace import Span, Status, StatusCode

import opengeh_utilities.telemetry.logging_configuration as config


def span_record_exception(exception: SystemExit | Exception, span: Span) -> None:
    span.set_status(Status(StatusCode.ERROR))
    span.record_exception(
        exception,
        attributes=config.get_extras() | {"CategoryName": f"Energinet.DataHub.{__name__}"},
    )
