L1.pig- feature tested: Type casting from bytes to bag.   parameters:
$PIG_HOME/bin/pig -p PIGMIX_JAR=$PIG_HOME/pigperf.jar  -p PARALLEL=10 -p PIGMIX_OUTPUT=/pigmixresults  $PIG_HOME/test/perf/pigmix/src/pig/L1.pig

L2.pig- feature tested: REPLICATED_JOIN. parameters:
pig -p PIGMIX_JAR=$PIG_HOME/pigperf.jar  -p PARALLEL=7 -p PIGMIX_OUTPUT=/pigmixresults  $PIG_HOME/test/perf/pigmix/src/pig/L2.pig

