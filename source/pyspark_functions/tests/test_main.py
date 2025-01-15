from pyspark_functions import main


def test_main():
    assert main() == "Hello from pyspark_functions!", "Should be 'Hello from pyspark_functions!'"
    