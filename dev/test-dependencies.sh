#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -e

FWDIR="$(cd "`dirname $0`"/..; pwd)"
cd "$FWDIR"

# Explicitly set locale in order to make `sort` output consistent across machines.
# See https://stackoverflow.com/questions/28881 for more details.
export LC_ALL=C

# NOTE: These should match those in the release publishing script
HADOOP2_MODULE_PROFILES="-Pmesos -Pkubernetes -Pyarn -Phive"
SBT="build/sbt"
HADOOP_PROFILES=(
    hadoop-2.7
    hadoop-3.2
)

# Generate manifests for each Hadoop profile:
for HADOOP_PROFILE in "${HADOOP_PROFILES[@]}"; do
  echo "Generating dependency manifest for $HADOOP_PROFILE"
  mkdir -p dev/pr-deps
  $SBT $HADOOP2_MODULE_PROFILES -P$HADOOP_PROFILE "show assembly/compile:dependencyClasspath" \
    | grep "Attributed(" \
    | awk -F "/" '{print $NF}' | sed 's/'\)'//g' | sort \
    | grep -v spark > dev/pr-deps/spark-deps-$HADOOP_PROFILE
done

if [[ $@ == **replace-manifest** ]]; then
  echo "Replacing manifests and creating new files at dev/deps"
  rm -rf dev/deps
  mv dev/pr-deps dev/deps
  exit 0
fi

for HADOOP_PROFILE in "${HADOOP_PROFILES[@]}"; do
  set +e
  dep_diff="$(
    git diff \
    --no-index \
    dev/deps/spark-deps-$HADOOP_PROFILE \
    dev/pr-deps/spark-deps-$HADOOP_PROFILE \
  )"
  set -e
  if [ "$dep_diff" != "" ]; then
    echo "Spark's published dependencies DO NOT MATCH the manifest file (dev/spark-deps)."
    echo "To update the manifest file, run './dev/test-dependencies.sh --replace-manifest'."
    echo "$dep_diff"
    rm -rf dev/pr-deps
    exit 1
  fi
done

exit 0
