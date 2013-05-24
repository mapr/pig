register $PIGMIX_JAR
A = load '/pigmix/pages625m' using org.apache.pig.test.pigmix.udf.PigPerformanceLoader()
	as (user, action, timespent, query_term, ip_addr, timestamp, estimated_revenue, page_info, page_links);
B = foreach A generate user, estimated_revenue;
alpha = load '/pigmix/power_users_samples' using PigStorage('\u0001') as (name, phone, address, city, state, zip);
beta = foreach alpha generate name, phone;
C = join B by user left outer, beta by name parallel $PARALLEL;
store C into '$PIGMIX_OUTPUT/L13out';

