Note:

export PIG_HOME=/opt/mapr/pig/pig-0.12
export PIG_VERSION=0.12.1
export pigjar=$PIG_HOME/pig-0.12.1-mapr-SNAPSHOT-withouthadoop.jar
export MAPPERS=10
export REDUCERS=7
$PIG_HOME/test/perf/pigmix/bin/generate_data_0.12.sh -r 325000 -w 10000 -u 500


--------------------------------------------------------------------------------------

L1.pig- feature tested: Type casting from bytes to bag.   parameters:
$PIG_HOME/bin/pig -p PIGMIX_JAR=$PIG_HOME/pigperf.jar  -p PARALLEL=10 -p PIGMIX_OUTPUT=/pigmixresults  $PIG_HOME/test/perf/pigmix/src/pig/L1.pig

L2.pig- feature tested: REPLICATED_JOIN. parameters:
pig -p PIGMIX_JAR=$PIG_HOME/pigperf.jar  -p PARALLEL=7 -p PIGMIX_OUTPUT=/pigmixresults  $PIG_HOME/test/perf/pigmix/src/pig/L2.pig
--------------------------------------------------------------------------------------

Script to runpigmix with example usage- 
./runpigmix.pl /opt/mapr/pig/pig-0.12 /opt/mapr/pig/pig-0.12/bin/pig  /opt/mapr/pig/pig-0.12/pigperf.jar /opt/mapr/hadoop/hadoop-0.20.2 /opt/mapr/hadoop/hadoop-0.20.2/bin/hadoop $PIG_HOME/test/perf/pigmix/src/pig /pigmix /pigmixresults


