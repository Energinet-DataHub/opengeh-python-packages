import logging
import os
import subprocess
import sys


def main():
    streamlit_script = os.path.join(os.path.dirname(__file__), "streamlit_app.py")
    command = ["streamlit", "run", streamlit_script] + sys.argv[1:]
    logging.warning(command)
    subprocess.run(command, check=True)


if __name__ == "__main__":
    main()
