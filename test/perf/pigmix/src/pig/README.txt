Note:
We generated pig data using the following script on a 15-node hadoop test cluster

export PIG_HOME=/opt/mapr/pig/pig-0.11.2
export PIG_VERSION=0.11.2
export pigjar=$PIG_HOME/pig-0.11.2-mapr-SNAPSHOT-core.jar
export MAPPERS=108
export REDUCERS=76
$PIG_HOME/test/perf/pigmix/bin/generate_data_0.11.2.sh -r 325000000 -w 10000000 -u 500

The above script runs for 8 hours and could run longer on smaller clusters and vice-versa.

--------------------------------------------------------------------------------------

L1.pig- feature tested: Type casting from bytes to bag.   parameters:
$PIG_HOME/bin/pig -p PIGMIX_JAR=$PIG_HOME/pigperf.jar  -p PARALLEL=10 -p PIGMIX_OUTPUT=/pigmixresults  $PIG_HOME/test/perf/pigmix/src/pig/L1.pig

L2.pig- feature tested: REPLICATED_JOIN. parameters:
pig -p PIGMIX_JAR=$PIG_HOME/pigperf.jar  -p PARALLEL=7 -p PIGMIX_OUTPUT=/pigmixresults  $PIG_HOME/test/perf/pigmix/src/pig/L2.pig
--------------------------------------------------------------------------------------

Script to runpigmix with example usage- 
./runpigmix.pl /opt/mapr/pig/pig-0.11.2 /opt/mapr/pig/pig-0.11.2/bin/pig  /opt/mapr/pig/pig-0.11.2/pigperf.jar /opt/mapr/hadoop/hadoop-0.20.2 /opt/mapr/hadoop/hadoop-0.20.2/bin/hadoop $PIG_HOME/test/perf/pigmix/src/pig /pigmixresults


