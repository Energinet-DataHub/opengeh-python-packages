#!/bin/sh -l

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

# Exit immediately with failure status if any command fails
set -e
cd source/

# There env vars are important to ensure that the driver and worker nodes in spark are alligned
export PYSPARK_PYTHON=/opt/conda/bin/python
export PYSPARK_DRIVER_PYTHON=/opt/conda/bin/python

for dir in $(find . -maxdepth 1 -type d);
do
    echo "Running tests in $dir"

    cd $dir

    coverage run --branch -m pytest --junitxml=pytest-results.xml
    # Create data for threshold evaluation
    coverage json
    # Create human reader friendly HTML report
    coverage html
    coverage-threshold --line-coverage-min 25

    cd ..
done
