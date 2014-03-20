#!/bin/sh
export HADOOP_HOME=/opt/local/hadoop

PACKAGE=target/giraph-samples-0.1.jar
MAIN_CLASS=giraph.samples.MyVertex
INPUT_FORMAT=giraph.samples.MyTextVertexInputFormat
OUTPUT_FORMAT=org.apache.giraph.io.formats.IdWithValueTextOutputFormat

DATA_PATH=/user/hadoop-mobile/minglin
INPUT_PATH=$DATA_PATH/input
OUTPUT_PATH=$DATA_PATH/output

$HADOOP_HOME/bin/hadoop jar $PACKAGE org.apache.giraph.GiraphRunner $MAIN_CLASS -vif $INPUT_FORMAT -vip $INPUT_PATH -of $OUTPUT_FORMAT -op $OUTPUT_PATH
