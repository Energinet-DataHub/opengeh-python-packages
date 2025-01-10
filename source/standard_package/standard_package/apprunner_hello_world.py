from Apprunner import AppInterface, AppRunner, LoggingSettings, AppMeta
import inspect
import pytest

class Application(AppInterface):
    def run(self):
        print("I am an app that is running.")
        print(" I adhere to the interface because I implement a run method")

class NonWorkingApplication(AppInterface):
    def runz(self):
        print("I am a non-working app.")
        print("I do not adhere to the interface because I implement a runz method")

# Define the logging settings
log_settings = LoggingSettings(
    cloud_role_name="MyAppRole",
    subsystem="MySubsystem",
    applicationinsights_connection_string="InstrumentationKey=XXXX-XXXX-XXXX",
    logging_extras={"key1": "value1", "key2": "value2"}
)

app_working = Application()
app_not_working = NonWorkingApplication()

print("Printing methods for app_not_working")
methods = [func for func, _ in inspect.getmembers(app_not_working, predicate=inspect.ismethod)]
print(methods)

print("Printing methods for app_working")
methods = [func for func, _ in inspect.getmembers(app_working, predicate=inspect.ismethod)]
print(methods)

AppRunner.run(app_working, log_settings)
# Test that it is actually failing
with pytest.raises(NotImplementedError):
    AppRunner.run(app_not_working, log_settings)


