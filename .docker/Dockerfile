# The spark version should should follow the spark version in databricks.
FROM jupyter/pyspark-notebook:spark-3.3.0

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

USER root
# This replaces the default spark configuration in the docker image with the ones defined in the sibling file
COPY spark-defaults.conf $SPARK_HOME/conf/spark-defaults.conf


# Install Azurite (support wasb-protocol storage for Delta tables in integration tests)
RUN apt-get update; \
    apt-get install -y npm && npm install -g n && n lts && hash -r && npm install -g azurite

# Install spark packages with mamba
RUN mamba install --quiet --yes --satisfied-skip-solve \
    'rope=1.6.*' 'pytest=7.2.*' 'configargparse=1.5.3'  \
    'coverage=7.0.*' 'pytest-mock=3.10.*' \
    'virtualenv=20.*'

# Install python packages used in pyspark development
COPY requirements.txt requirements.txt
RUN pip --no-cache-dir install -r requirements.txt

# Below will make everything in the directory owned by the group ${NB_GID}
RUN fix-permissions "${CONDA_DIR}"

# Set misc environment variables required for properly run spark
# note the amount of memory used on the driver is adjusted here
ENV PATH=$SPARK_HOME/bin:$HADOOP_HOME/bin:$PATH \
    PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.5-src.zip" \
    SPARK_OPTS="--driver-java-options=-Xms1024M --driver-java-options=-Xmx4096M --driver-java-options=-Dlog4j.logLevel=info"

# Dynamically downloading spark dependencies. This is done to save time in the build pipeline so that we don't need to download on every build.
RUN spark-shell -conf spark-defaults.conf

# Make $HOME owned by the root, which is the user used in the container
# This is needed for e.g. commands that create files or folders in $HOME
RUN sudo chown -R root:users $HOME
