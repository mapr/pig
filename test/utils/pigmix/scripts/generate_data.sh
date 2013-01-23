#!/bin/sh

set -x 

java=${JAVA_HOME:='/usr'}/bin/java;
hadoop_ops="-Djava.library.path=/opt/mapr/lib -Dmapred.map.child.java.opts=-Xmx2000m -Dmapred.reduce.child.java.opts=-Xmx2000m -Dio.sort.mb=350"

if [ ! -e $java ] 
then
    echo "Cannot find java, set JAVA_HOME environment variable to directory above bin/java."
    exit
fi

PIG_HOME=$(echo /opt/mapr/pig/pig-*)
HADOOP_HOME="/opt/mapr/hadoop/hadoop-0.20.2"
pigjar=$PIG_HOME/pig.jar

testjar=$PIG_HOME/pigperf.jar
PIG_DATA="/pigmix"
PIG_RESULTS="/pigmixresults"
MAPRED_PIG_RESULTS="/mapredpigmixresults"
if [ ! -e $pigjar ]
then
    echo "Cannot find pig.jar, set PIG_HOME environment variable to directory pig.jar and build directory are in"
    exit
fi

echo "Creating data and results volumes and directories"
maprcli volume create -name pigdata -path $PIG_DATA -replication 3
maprcli volume create -name pigresults -path $PIG_RESULTS -replication 3
maprcli volume create -name mapredpigresults -path $MAPRED_PIG_RESULTS -replication 3

#zipfjar=$PIG_HOME/lib/sdsuLibJKD12.jar

maprfsjar=$HADOOP_HOME/lib/maprfs-0.1.jar

hadoopjar=$HADOOP_HOME/lib/hadoop-0.20.2-dev-core.jar

zookeeperjar="/opt/mapr/zookeeper/zookeeper-3.3.2/zookeeper-3.3.2.jar"

# configure the cluster
conf_dir=$HADOOP_HOME/conf

pig_conf=$PIG_HOME/conf/pig.properties

classpath=$hadoopjar:$pigjar:$pig_conf:$testjar:$maprfsjar:$zookeeperjar:$conf_dir:lib/*

export HADOOP_CLASSPATH=$classpath

# configure the number of mappers, if 0, job is run locally
mappers=100

mainclass=org.apache.pig.test.utils.datagen.DataGenerator 

rows=625000000

user_field=s:20:1600000:z:7
action_field=i:1:2:u:0
os_field=i:1:20:z:0
query_term_field=s:10:1800000:z:20
ip_addr_field=l:1:1000000:z:0
timestamp_field=l:1:86400:z:0
estimated_revenue_field=d:1:100000:z:5
page_info_field=m:10:1:z:0
page_links_field=bm:10:1:z:20

pages="/pigmix/pages625m"

echo "Generating $pages"

$HADOOP_HOME/bin/hadoop --config $conf_dir jar $testjar $mainclass $hadoop_ops \
    -m $mappers -r $rows -f $pages $user_field \
    $action_field $os_field $query_term_field $ip_addr_field \
    $timestamp_field $estimated_revenue_field $page_info_field \
    $page_links_field


# Skim off 1 in 10 records for the user table
# Be careful the file is in HDFS if you run previous job as hadoop job, 
# you should either copy data into local disk to run following script
# or run hadoop job to trim the data

protousers="$PIG_DATA/users"
echo "Skimming users"
java $hadoop_ops -cp $classpath org.apache.pig.Main << EOF
register $PIG_HOME/pigperf.jar;
fs -rmr '$protousers';
A = load '$pages' using org.apache.pig.test.udf.storefunc.PigPerformanceLoader()
    as (user, action, timespent, query_term, ip_addr, timestamp, estimated_revenue, page_info, page_links);
B = foreach A generate user;
C = distinct B parallel $mappers;
D = order C by \$0 parallel $mappers;
store D into '$protousers';
EOF


# Create users table, with now user field.
phone_field=s:10:1600000:z:20
address_field=s:20:1600000:z:20
city_field=s:10:1600000:z:20
state_field=s:2:1600:z:20
zip_field=i:2:1600:z:20

users="$PIG_DATA/users100m"

echo "Generating $users"

$HADOOP_HOME/bin/hadoop --config $conf_dir jar $testjar $mainclass $hadoop_ops \
    -m $mappers -i $protousers -f $users $phone_field \
    $address_field $city_field $state_field $zip_field

# Find unique keys for fragment replicate join testing
# If file is in HDFS, extra steps are required
numuniquekeys=500
protopowerusers="$PIG_DATA/power_users"
echo "Skimming power users"

java $hadoop_ops -cp $classpath org.apache.pig.Main << EOF
register $PIG_HOME/pigperf.jar;
fs -rmr $protopowerusers;
A = load '$pages' using org.apache.pig.test.udf.storefunc.PigPerformanceLoader()
    as (user, action, timespent, query_term, ip_addr, timestamp, estimated_revenue, page_info, page_links);
B = foreach A generate user;
C = distinct B parallel $mappers;
D = order C by \$0 parallel $mappers;
E = limit D 500;
store E into '$protopowerusers';
EOF

echo "Generating $powerusers"

powerusers="$PIG_DATA/power_users100m"

$HADOOP_HOME/bin/hadoop --config $conf_dir jar $testjar $mainclass $hadoop_ops \
    -m $mappers -i $protopowerusers -f $powerusers $phone_field \
    $address_field $city_field $state_field $zip_field

echo "Generating widerow"

widerowcnt=10000000
widerows="$PIG_DATA/widerow"
user_field=s:20:10000:z:0
int_field=i:1:10000:u:0 

$HADOOP_HOME/bin/hadoop --config $conf_dir jar $testjar $mainclass $hadoop_ops \
    -m $mappers -r $widerowcnt -f $widerows $user_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field

java $hadoop_ops -cp $classpath org.apache.pig.Main << EOF
register $PIG_HOME/pigperf.jar;
fs -rmr ${pages}_sorted;
fs -rmr ${users}_sorted;
fs -rmr ${powerusers}_samples;
fs -rmr ${protopowerusers};
A = load '$pages' using org.apache.pig.test.udf.storefunc.PigPerformanceLoader()
    as (user, action, timespent, query_term, ip_addr, timestamp, estimated_revenue, page_info, page_links);
B = order A by user parallel $mappers;
store B into '${pages}_sorted' using PigStorage('\u0001');

alpha = load '$users' using PigStorage('\u0001') as (name, phone, address, city, state, zip);
a1 = order alpha by name parallel $mappers;
store a1 into '${users}_sorted' using PigStorage('\u0001');

a = load '$powerusers' using PigStorage('\u0001') as (name, phone, address, city, state, zip);
b = sample a 0.5;
store b into '${powerusers}_samples' using PigStorage('\u0001');

A = load '$pages' as (user, action, timespent, query_term, ip_addr, timestamp,
        estimated_revenue, page_info, page_links);
B = foreach A generate user, action, timespent, query_term, ip_addr, timestamp, estimated_revenue, page_info, page_links,
user as user1, action as action1, timespent as timespent1, query_term as query_term1, ip_addr as ip_addr1, timestamp as timestamp1, estimated_revenue as estimated_revenue1, page_info as page_info1, page_links as page_links1,
user as user2, action as action2, timespent as timespent2, query_term as query_term2, ip_addr as ip_addr2, timestamp as timestamp2, estimated_revenue as estimated_revenue2, page_info as page_info2, page_links as page_links2;
store B into '$PIG_DATA/widegroupbydata';
EOF

mkdir -p $powerusers
java $hadoop_ops -cp $classpath org.apache.pig.Main << EOF
fs -copyToLocal ${powerusers}/part-* $powerusers;
EOF

cat $powerusers/* > power_users

java $hadoop_ops -cp $classpath org.apache.pig.Main << EOF
fs -copyFromLocal power_users /pigmix/power_users;
EOF

rm -fr $powerusers
rm power_users

set +x
