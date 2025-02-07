#! /bin/bash
set -e
docker buildx build --platform linux/amd64 -t pyspark-minimal:latest .