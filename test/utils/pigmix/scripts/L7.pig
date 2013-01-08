-- This script covers having a nested plan with splits.
register ../../../../pigperf.jar;
A = load '/pigmix/pages625m' using org.apache.pig.test.udf.storefunc.PigPerformanceLoader() as (user, action, timespent, query_term,
            ip_addr, timestamp, estimated_revenue, page_info, page_links);
B = foreach A generate user, timestamp;
C = group B by user parallel $reducers;
D = foreach C {
    morning = filter B by timestamp < 43200;
    afternoon = filter B by timestamp >= 43200;
    generate group, COUNT(morning), COUNT(afternoon);
}
store D into '/pigmixresults/L7out';
