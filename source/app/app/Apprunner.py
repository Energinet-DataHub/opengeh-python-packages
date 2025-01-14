from dataclasses import dataclass
from opentelemetry.trace import SpanKind
from telemetry_logging import logging_configuration as config
from source.telemetry.telemetry_logging.logging_configuration import LoggingSettings


class AppMeta(type):
    """
        A metaclass for defining structural behavior for classes implementing the `AppInterface`.
        This metaclass overrides type-checking behavior to enforce that any class or instance
        considered a subclass or instance of `AppInterface` must define a callable `run` method
    """

    def __instancecheck__(cls, instance):
        """
        Determines if the given instance is considered an instance of a class using the metaclass AppMeta
        """
        return cls.__subclasscheck__(type(instance))

    def __subclasscheck__(cls, subclass):
        """
        Determines if the given subclass adheres to the structural requirements of the interface.
        E.g. it needs a run method
        We check if the AppInterface is in the Method Resolution Order, meaning that we explicitly check if the subclass
        has declared AppInterface as a base class directly or through inheritance.
        """
        return (hasattr(subclass, "run") and
                callable(subclass.run)
                and AppInterface in subclass.__mro__ # Method Resolution Order (MRO)
                )


class AppInterface(metaclass=AppMeta):
    """
    A base class for all applications that enforces the `AppMeta` structural contract.
    Any class that implements this interface must define a callable `run` method to be
    considered a valid subclass or instance of `AppInterface`.
    """

    def run(self):
        """
        All classes implementing this interface must implement the run method.
        Otherwise, it will raise an NotImplementedError.
        """
        raise NotImplementedError("Subclasses must implement the 'run' method.")

class AppRunner:
    """
     A util class for running applications that implement the `AppInterface`.

     The `AppRunner` class provides a `run` method to initialize and execute an application,
     ensuring that it adheres to the structural contract defined by `AppInterface`.
     It also specifies an argument for providing logging settings for the application.

     Methods:
         run(cls, app: AppInterface, logging_settings: LoggingSettings):
             A class method that verifies the application implements `AppInterface`,
             configures logging, and runs the application.
     """
    @classmethod
    def run(
        cls, # Class method taking itself as an argument
        app: AppInterface,
        logging_settings: LoggingSettings,
    ):
        """
        Check if the provided application implements the `AppInterface` and calls private run function.

        The isinstance check is important, as it delegates to __subclasscheck__ in the AppMeta class.
        Hereby, we can verify, that classes that do not
        """
        if not isinstance(app, AppInterface):
            raise TypeError("'app' must be a subclass of 'AppInterface'")

        _run(app, logging_settings)


def _run(
    app: AppInterface,
    logging_settings: LoggingSettings,
) -> None:

    config.configure_logging(
        cloud_role_name=logging_settings.cloud_role_name,
        tracer_name=logging_settings.tracer_name,
        applicationinsights_connection_string=logging_settings.applicationinsights_connection_string,
        extras=logging_settings.logging_extras,
        force_configuration=logging_settings.force_configuration,
    )

    with config.get_tracer().start_as_current_span(__name__, kind=SpanKind.SERVER):
        app.run()
