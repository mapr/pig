register $PIGMIX_JAR
A = load '/pigmix/pages625m_sorted' using org.apache.pig.test.pigmix.udf.PigPerformanceLoader()
    as (user, action, timespent, query_term, ip_addr, timestamp, estimated_revenue, page_info, page_links);
B = foreach A generate user, estimated_revenue;
alpha = load '/pigmix/users100m_sorted' using PigStorage('\u0001') as (name, phone, address, city, state, zip);
beta = foreach alpha generate name;
C = join B by user, beta by name using 'merge';
store C into '$PIGMIX_OUTPUT/L14out';

