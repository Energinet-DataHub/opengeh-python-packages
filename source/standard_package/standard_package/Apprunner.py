from dataclasses import dataclass
#import telemetry_logging.logging_configuration as config #Needs to be implemented in the telemetry logging_logging package
from opentelemetry.trace import SpanKind


class AppMeta(type):
    """
        A metaclass for defining structural behavior for classes implementing the `AppInterface`.
        This metaclass overrides type-checking behavior to enforce that any class or instance
        considered a subclass or instance of `AppInterface` must define a callable `run` method,
        along other methods.
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
        """
        return hasattr(subclass, "run") and callable(subclass.run)


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

# TODO: Move to telemetry package
@dataclass
class LoggingSettings:
    cloud_role_name: str
    applicationinsights_connection_string: str
    subsystem: str
    logging_extras: dict = None

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
        """Check if the provided application implements the `AppInterface` and calls private"""
        if not isinstance(app, AppInterface):
            raise TypeError("'app' must be a subclass of 'AppInterface'")

        _run(app, logging_settings)


def _run(
    app: AppInterface,
    logging_settings: LoggingSettings,
) -> None:
    # TODO: Add configure_logging(logging_settings) function to telemetry package
    #config.configure_logging(
    #    cloud_role_name=logging_settings.cloud_role_name,
    #    tracer_name=logging_settings.subsystem,
    #    applicationinsights_connection_string=logging_settings.applicationinsights_connection_string,
    #    extras=logging_settings.logging_extras,
    #)
    print(logging_settings.cloud_role_name)
    print(logging_settings.applicationinsights_connection_string)
    print(logging_settings.subsystem)
    print(logging_settings.logging_extras)

    #with config.get_tracer().start_as_current_span(__name__, kind=SpanKind.SERVER):
    app.run()
