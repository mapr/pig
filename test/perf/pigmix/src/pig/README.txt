Note:
export PIG_HOME=/opt/mapr/pig/pig-0.14
export MAPPERS=10
export REDUCERS=7

on mapr-core 4.x
sudo -u mapr bash $PIG_HOME/test/perf/pigmix/bin/generate_data_0.14.sh -r 325000 -w 10000 -u 500

on mapr-core 3.x
sudo -u mapr bash  $PIG_HOME/test/perf/pigmix/bin/generate_data_3.x_0.14.sh -r 325000 -w 10000 -u 500

--------------------------------------------------------------------------------------

L1.pig- feature tested: Type casting from bytes to bag. parameters:
$PIG_HOME/bin/pig -p PIGMIX_JAR=$PIG_HOME/pigperf.jar -p PARALLEL=10 -p PIGMIX_OUTPUT=/pigmixresults $PIG_HOME/test/perf/pigmix/src/pig/L1.pig

L2.pig- feature tested: REPLICATED_JOIN. parameters:
pig -p PIGMIX_JAR=$PIG_HOME/pigperf.jar -p PARALLEL=7 -p PIGMIX_OUTPUT=/pigmixresults $PIG_HOME/test/perf/pigmix/src/pig/L2.pig
--------------------------------------------------------------------------------------

Script to runpigmix with example usage on MR1 (or yarn configured to run on MR1) -

./runpigmix.pl /opt/mapr/pig/pig-0.14 /opt/mapr/pig/pig-0.14/bin/pig /opt/mapr/pig/pig-0.14/pigperf-h1.jar /opt/mapr/hadoop/hadoop-0.20.2 /opt/mapr/hadoop/hadoop-0.20.2/bin/hadoop $PIG_HOME/test/perf/pigmix/src/pig /pigmix /pigmixresults

Script to runpigmix with example usage on MR2 (yarn)-

./runpigmix.pl /opt/mapr/pig/pig-0.14 /opt/mapr/pig/pig-0.14/bin/pig /opt/mapr/pig/pig-0.14/pigperf-h2.jar /opt/mapr/hadoop/hadoop-2.4.1 /opt/mapr/hadoop/hadoop-2.4.1/bin/hadoop $PIG_HOME/test/perf/pigmix/src/pig /pigmix /pigmixresults
