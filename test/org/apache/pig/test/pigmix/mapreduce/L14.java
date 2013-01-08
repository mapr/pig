package org.apache.pig.test.pigmix.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapred.lib.IdentityMapper;

import org.apache.pig.test.pigmix.mapreduce.Library;

public class L14 {

    public static class ReadPageViews extends MapReduceBase
        implements Mapper<LongWritable, Text, Text, Text> {

        public void map(
                LongWritable k,
                Text val,
                OutputCollector<Text, Text> oc,
                Reporter reporter) throws IOException {

            List<Text> fields = Library.splitLine(val, '');
            // Prepend an index to the value so we know which file
            // it came from.
            Text outVal = new Text("1" + fields.get(6).toString());
            oc.collect(fields.get(0), outVal);
        }
    }

    public static class ReadUsers extends MapReduceBase
        implements Mapper<LongWritable, Text, Text, Text> {

        public void map(
                LongWritable k,
                Text val,
                OutputCollector<Text, Text> oc,
                Reporter reporter) throws IOException {
            List<Text> fields = Library.splitLine(val, '\u0001');
            // Prepend an index to the value so we know which file
            // it came from.
            Text outVal = new Text("2");
            oc.collect(fields.get(0), outVal);

        }
    }

    public static class Join extends MapReduceBase
        implements Reducer<Text, Text, Text, Text> {

        public void reduce(
                Text key,
                Iterator<Text> iter, 
                OutputCollector<Text, Text> oc,
                Reporter reporter) throws IOException {
            // For each value, figure out which file it's from and store it
            // accordingly.
            List<String> first = new ArrayList<String>();
            List<String> second = new ArrayList<String>();

            while (iter.hasNext()) {
                Text t = iter.next();
                String value = t.toString();
                if (value.charAt(0) == '1') first.add(value.substring(1));
                else second.add(value.substring(1));
                reporter.setStatus("OK");
            }

            reporter.setStatus("OK");

            if (first.size() == 0 || second.size() == 0) return;

            // Do the cross product, and calculate the sum
            for (String s1 : first) {
                for (String s2 : second) {
                    try {
                        oc.collect(null, new Text(key.toString() + "\t" + s1 + "\t" + key.toString()));
                    } catch (NumberFormatException nfe) {
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws IOException {

        String user = System.getProperty("user.name");
        JobConf lp = new JobConf(L14.class);
        lp.setJobName("L14:Load Page Views");
        lp.setInputFormat(TextInputFormat.class);
        lp.setOutputKeyClass(Text.class);
        lp.setOutputValueClass(Text.class);
        lp.setMapperClass(ReadPageViews.class);
        Properties props = System.getProperties();
        for (Map.Entry<Object,Object> entry : props.entrySet()) {
            lp.set((String)entry.getKey(), (String)entry.getValue());
        }
        FileInputFormat.addInputPath(lp, new Path("/pigmix/pages625m_sorted"));
        FileOutputFormat.setOutputPath(lp, new Path("/user/"+user+"/tmp/l14indexed_pages"));
        lp.setNumReduceTasks(0);
        Job loadPages = new Job(lp);

        JobConf lu = new JobConf(L14.class);
        lu.setJobName("L14:Load Users");
        lu.setInputFormat(TextInputFormat.class);
        lu.setOutputKeyClass(Text.class);
        lu.setOutputValueClass(Text.class);
        lu.setMapperClass(ReadUsers.class);
        props = System.getProperties();
        for (Map.Entry<Object,Object> entry : props.entrySet()) {
            lu.set((String)entry.getKey(), (String)entry.getValue());
        }
        FileInputFormat.addInputPath(lu, new Path("/pigmix/users100m_sorted"));
        FileOutputFormat.setOutputPath(lu, new Path("/user/"+user+"/tmp/l14indexed_users"));
        lu.setNumReduceTasks(0);
        Job loadUsers = new Job(lu);

        JobConf join = new JobConf(L14.class);
        join.setJobName("L14:Join Users and Pages");
        join.setInputFormat(KeyValueTextInputFormat.class);
        join.setOutputKeyClass(Text.class);
        join.setOutputValueClass(Text.class);
        join.setMapperClass(IdentityMapper.class);
        join.setReducerClass(Join.class);
        props = System.getProperties();
        for (Map.Entry<Object,Object> entry : props.entrySet()) {
            join.set((String)entry.getKey(), (String)entry.getValue());
        }
        FileInputFormat.addInputPath(join, new Path("/user/"+user+"/tmp/l14indexed_pages"));
        FileInputFormat.addInputPath(join, new Path("/user/"+user+"/tmp/l14indexed_users"));
        FileOutputFormat.setOutputPath(join, new Path("/mapredpigmixresults/L14out"));
        //join.setNumReduceTasks(4);
        Job joinJob = new Job(join);
        joinJob.addDependingJob(loadPages);
        joinJob.addDependingJob(loadUsers);

        JobControl jc = new JobControl("L14 join");
        jc.addJob(loadPages);
        jc.addJob(loadUsers);
        jc.addJob(joinJob);

        new Thread(jc).start();
   
        int i = 0;
        while(!jc.allFinished()){
            ArrayList<Job> failures = jc.getFailedJobs();
            if (failures != null && failures.size() > 0) {
                for (Job failure : failures) {
                    System.err.println(failure.getMessage());
                }
                break;
            }

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {}

            if (i % 10000 == 0) {
                System.out.println("Running jobs");
                ArrayList<Job> running = jc.getRunningJobs();
                if (running != null && running.size() > 0) {
                    for (Job r : running) {
                        System.out.println(r.getJobName());
                    }
                }
                System.out.println("Ready jobs");
                ArrayList<Job> ready = jc.getReadyJobs();
                if (ready != null && ready.size() > 0) {
                    for (Job r : ready) {
                        System.out.println(r.getJobName());
                    }
                }
                System.out.println("Waiting jobs");
                ArrayList<Job> waiting = jc.getWaitingJobs();
                if (waiting != null && waiting.size() > 0) {
                    for (Job r : ready) {
                        System.out.println(r.getJobName());
                    }
                }
                System.out.println("Successful jobs");
                ArrayList<Job> success = jc.getSuccessfulJobs();
                if (success != null && success.size() > 0) {
                    for (Job r : ready) {
                        System.out.println(r.getJobName());
                    }
                }
            }
            i++;
        }
        ArrayList<Job> failures = jc.getFailedJobs();
        if (failures != null && failures.size() > 0) {
            for (Job failure : failures) {
                System.err.println(failure.getMessage());
            }
        }
        jc.stop();
    }

}
