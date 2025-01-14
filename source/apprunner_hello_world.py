from source.app.app.Apprunner import AppInterface, AppRunner
import inspect
import pytest

from source.telemetry.telemetry_logging import logging_configuration as logging_config, decorators as logging_decorators

"""
New development approach: testing the logging setup using the decorator approach
"""

# Define the logging settings used in all the tests we do
log_settings = logging_config.LoggingSettings(
    cloud_role_name="MyAppRole",
    tracer_name="MyTracerName",
    applicationinsights_connection_string=None,
    logging_extras={"key1": "value1", "key2": "value2"},
    force_configuration=False
)

# Creates classes we need for testing

class Application(AppInterface):
    def run(self):
        print("I am an app that is running.")
        print(" I adhere to the interface because I implement a run method")

class NonWorkingApplication(AppInterface):
    def runz(self):
        print("I am a non-working app.")
        print("I do not adhere to the interface because I implement a runz method")

class FakeApplicationClass():
    variable1: str
    def runz(self):
        print("I am a fake class.")

app_working = Application()
app_not_working = NonWorkingApplication()

print("Printing methods for app_not_working")
methods = [func for func, _ in inspect.getmembers(app_not_working, predicate=inspect.ismethod)]
print(methods)

print("Printing methods for app_working")
methods = [func for func, _ in inspect.getmembers(app_working, predicate=inspect.ismethod)]
print(methods)

AppRunner.run(app_working, log_settings)

# Test that it is actually failing when not overwriting the run() method
with pytest.raises(NotImplementedError):
    AppRunner.run(app_not_working, log_settings)

# Create a test that calls Apprunner.run with a non-AppInterface based class
fake_app = FakeApplicationClass()

with pytest.raises(TypeError):
    AppRunner.run(fake_app, log_settings)



"""
New development approach: testing the logging setup using the decorator approach
"""
# get_tracer does not have _TRACER_NAME yet
log_settings = logging_config.LoggingSettings(
    cloud_role_name="MyAppRole",
    tracer_name="MyTracerName",
    applicationinsights_connection_string=None,
    logging_extras={"key1": "value1", "key2": "value2"},
    force_configuration=False
)

logging_settings = log_settings
logging_config.configure_logging(
        cloud_role_name=logging_settings.cloud_role_name,
        tracer_name=logging_settings.tracer_name,
        applicationinsights_connection_string=logging_settings.applicationinsights_connection_string,
        extras=logging_settings.logging_extras,
        force_configuration=logging_settings.force_configuration,
    )

# Now we are testing the new decorator functionality for our apps
class SimpleApp():
    @logging_decorators.use_logging
    def run(self):
        print("I am a new app and this is my functionality")

print("config.configure_logging has been called")
newAppInstance = SimpleApp()
newAppInstance.run()



