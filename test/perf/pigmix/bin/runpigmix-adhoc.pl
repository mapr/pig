#!/usr/local/bin/perl -w

if(scalar(@ARGV) < 3 )
{
    print STDERR "Usage: $0 <pigjar> <pigperfjar> <pigdir> <dir containing hadoopsitexml> <pig mix scripts dir> [numruns] [runmapreduce] \n";
    exit(-1);
}
my $pigjar=shift;
my $pigperfjar=shift;
my $pighome=shift;
my $confdir=shift;
my $scriptdir = shift;
my $runs = shift;
my $reducers = 40;
my $runmapreduce = shift;
my $hadoop_opts = "-Djava.library.path=/opt/mapr/lib -Dmapred.map.child.java.opts=-Xmx1500m -Dmapred.reduce.child.java.opts=-Xmx2000m -Dio.sort.mb=350 -Dmapred.reduce.tasks=$reducers";

if(!defined($runs)) {
    $runs = 1;
}

if(!defined($runmapreduce)) {
    $runmapreduce = 0;
}

my $java;
my $java_home = $ENV{'JAVA_HOME'};
if(!defined($java_home)) {
    $java = "/usr/bin/java";
} else {
    $java = $java_home."/bin/java";
}

my $cmd;
my $total_pig_times = 0;
my $total_mr_times = 0;
my $HADOOP_HOME="/opt/mapr/hadoop/hadoop-0.20.2";
my $HADOOP_LIB="$HADOOP_HOME/lib";
my $hadoopjar="/opt/mapr/hadoop/hadoop-0.20.2/lib/hadoop-0.20.2-dev-core.jar";
my $marfsjar="/opt/mapr/hadoop/hadoop-0.20.2/lib/maprfs-1.0.3-mapr-3.0.0.jar";
my $zkjar="/opt/mapr/zookeeper/zookeeper-3.3.6/zookeeper-3.3.6.jar";
my $lib="$pighome/lib";
my $pigprops="$pighome/conf/pig.properties";
my $classpath="$pigperfjar:$pigprops:$hadoopjar:$pigjar:$confdir:$marfsjar:$zkjar:$lib/*:$HADOOP_LIB/*";
for(my $i = 1; $i < 17; $i++) {
    my $pig_times = 0;
    for(my $j = 0; $j < $runs; $j++) {
        print STDERR "Running Pig Query L".$i."\n";
        print STDERR "L".$i.":";
        print STDERR "Going to run $java $hadoop_opts -cp $classpath org.apache.pig.Main -param reducers=$reducers $scriptdir/L".$i.".pig\n";
        my $s = time();
        $cmd = "$java $hadoop_opts -cp $classpath org.apache.pig.Main -param reducers=$reducers $scriptdir/L". $i.".pig" ;
        print STDERR `$cmd 2>&1`;
        my $e = time();
        $pig_times += $e - $s;
        #cleanup($i);
    }
    # find avg
    $pig_times = $pig_times/$runs;
    # round to next second
    $pig_times = int($pig_times + 0.5);
    $total_pig_times = $total_pig_times + $pig_times;

    if ($runmapreduce==0) {
        print "PigMix_$i pig run time: $pig_times\n";
    }
    else {
        $mr_times = 0;
        for(my $j = 0; $j < $runs; $j++) {
            print STDERR "Running Map-Reduce Query L".$i."\n";
            print STDERR "L".$i.":";
            print STDERR "Going to run $java $hadoop_opts -cp $classpath org.apache.pig.test.pigmix.mapreduce.L"."$i\n";
            my $s = time();
            $cmd = "$java $hadoop_opts -cp $classpath org.apache.pig.test.pigmix.mapreduce.L$i";
            print STDERR `$cmd 2>&1`;
            my $e = time();
            $mr_times += $e - $s;
            cleanup($i);
        }
        # find avg
        $mr_times = $mr_times/$runs;
        # round to next second
        $mr_times = int($mr_times + 0.5);
        $total_mr_times = $total_mr_times + $mr_times;

        my $multiplier = $pig_times/$mr_times;
        print "PigMix_$i pig run time: $pig_times, java run time: $mr_times, multiplier: $multiplier\n";
    }
}

if ($runmapreduce==0) {
    print "Total pig run time: $total_pig_times\n";
}
else {
    my $total_multiplier = $total_pig_times / $total_mr_times;
    print "Total pig run time: $total_pig_times, total java time: $total_mr_times, multiplier: $total_multiplier\n";
}

sub cleanup {
    my $suffix = shift;
    my $cmd;
    $cmd = "$java -Djava.library.path=/opt/mapr/lib -cp $classpath org.apache.pig.Main -e rmf L".$suffix."out";
    print STDERR `$cmd 2>&1`;
    $cmd = "$java -Djava.library.path=/opt/mapr/lib -cp $classpath org.apache.pig.Main -e rmf highest_value_page_per_user";
    print STDERR `$cmd 2>&1`;
    $cmd = "$java -Djava.library.path=/opt/mapr/lib -cp $classpath org.apache.pig.Main -e rmf total_timespent_per_term";
    print STDERR `$cmd 2>&1`;
    $cmd = "$java -Djava.library.path=/opt/mapr/lib -cp $classpath org.apache.pig.Main -e rmf queries_per_action";
    print STDERR `$cmd 2>&1`;
    $cmd = "$java -Djava.library.path=/opt/mapr/lib -cp $classpath org.apache.pig.Main -e rmf tmp";
    print STDERR `$cmd 2>&1`;
}

