#!/bin/bash
 
# Usage: ./run_spark.sh <amazonCentersPath> <demandRegionsPath> <optimizerOutputPath> <numCandidates>
SPARK_MASTER_URL=spark://herring.cs.colostate.edu:7077
AMAZON_FILE=Data/AmazonCenters/amazonCenters.csv
DEMAND_FILE=Data/DemandRegion/demandRegions.csv
OUTPUT_FILE=output
NUM_CORES=40

NUM_CANDIDATES=$1
 
# Initialize modules system
module unload courses/cs455
module load java
source ~/.bashrc
$SPARK_HOME/sbin/stop-workers.sh
$SPARK_HOME/sbin/stop-master.sh
 
module unload java
module load courses/cs455
gradle clean build
 
module unload courses/cs455
module load java
source ~/.bashrc
$SPARK_HOME/sbin/start-master.sh
$SPARK_HOME/sbin/start-workers.sh
 
spark-submit \
  --class cs455.amazon.Controller \
  --master $SPARK_MASTER_URL \
  --deploy-mode client \
  --total-executor-cores $NUM_CORES \
  --executor-cores 2 \
  --executor-memory 2G \
  --conf spark.driver.memory=1G \
  --conf spark.dynamicAllocation.enabled=false \
  --conf spark.eventLog.enabled=false \
  build/libs/WhereToPut-It.jar \
  $AMAZON_FILE \
  $DEMAND_FILE \
  $OUTPUT_FILE \
  $NUM_CANDIDATES
 
 