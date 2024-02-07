import os
import re


def get_source_path() -> str:
    working_directory = os.getcwd()
    source_path = re.match(r"(.+?source)", working_directory).group(1)
    return source_path
