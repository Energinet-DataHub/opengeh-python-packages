#!/usr/bin/env python3
# Copyright 2020 Energinet DataHub A/S
#
# Licensed under the Apache License, Version 2.0 (the "License2");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Requirements:
# - Run as the root user
# - Required env variable: SPARK_HOME

import argparse
import logging
import os
import subprocess
import urllib.request
from pathlib import Path

import requests
from bs4 import BeautifulSoup

LOGGER = logging.getLogger(__name__)


def get_all_refs(url: str) -> list[str]:
    """Get all the references for a given webpage."""
    resp = requests.get(url)
    soup = BeautifulSoup(resp.text, "html.parser")
    return [a["href"] for a in soup.find_all("a", href=True)]


def get_latest_spark_version() -> str:
    """Return the last stable version of Spark using spark archive."""
    LOGGER.info("Downloading Spark versions information")
    all_refs = get_all_refs("https://archive.apache.org/dist/spark/")
    stable_versions = [
        ref.removeprefix("spark-").removesuffix("/")
        for ref in all_refs
        if ref.startswith("spark-") and "incubating" not in ref and "preview" not in ref
    ]
    # Compare versions semantically
    latest_version = max(stable_versions, key=lambda ver: [int(sub_ver) for sub_ver in ver.split(".")])
    LOGGER.info(f"Latest version: {latest_version}")
    return latest_version


def download_spark(
    spark_version: str,
    hadoop_version: str,
    scala_version: str,
    spark_download_url: str,
) -> str:
    """Download and unpack spark.

    The resulting spark directory name is returned
    """
    LOGGER.info("Downloading and unpacking Spark")
    spark_dir_name = f"spark-{spark_version}-bin-hadoop{hadoop_version}"
    if scala_version:
        spark_dir_name += f"-scala{scala_version}"
    LOGGER.info(f"Spark directory name: {spark_dir_name}")
    spark_url = f"{spark_download_url}/spark-{spark_version}/{spark_dir_name}.tgz"

    LOGGER.info(f"Downloading Spark from {spark_url}")
    tmp_file = Path("/tmp/spark.tar.gz")
    urllib.request.urlretrieve(spark_url, "/tmp/spark.tar.gz")

    subprocess.check_call(
        [
            "tar",
            "xzf",
            tmp_file,
            "-C",
            "/usr/local",
            "--owner",
            "root",
            "--group",
            "root",
            "--no-same-owner",
        ]
    )
    tmp_file.unlink()
    return spark_dir_name


def configure_spark(spark_dir_name: str, spark_home: Path) -> None:
    """Create a ${SPARK_HOME} symlink to a versioned spark directory.

    Creates a 10spark-config.sh symlink to source PYTHONPATH automatically
    """
    LOGGER.info("Configuring Spark")
    subprocess.check_call(["ln", "-s", f"/usr/local/{spark_dir_name}", spark_home])

    # Add a link in the before_notebook hook in order to source PYTHONPATH automatically
    CONFIG_SCRIPT = "/usr/local/bin/before-notebook.d/10spark-config.sh"
    subprocess.check_call(["ln", "-s", spark_home / "sbin/spark-config.sh", CONFIG_SCRIPT])


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("--spark-version", required=True)
    arg_parser.add_argument("--hadoop-version", required=True)
    arg_parser.add_argument("--scala-version", required=True)
    arg_parser.add_argument("--spark-download-url", type=str, required=True)
    args = arg_parser.parse_args()

    args.spark_version = args.spark_version or get_latest_spark_version()

    spark_dir_name = download_spark(
        spark_version=args.spark_version,
        hadoop_version=args.hadoop_version,
        scala_version=args.scala_version,
        spark_download_url=args.spark_download_url,
    )
    configure_spark(spark_dir_name=spark_dir_name, spark_home=Path(os.environ["SPARK_HOME"]))
