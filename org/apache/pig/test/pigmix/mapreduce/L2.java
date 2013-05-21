package org.apache.pig.test.pigmix.mapreduce;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Properties;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.pig.test.pigmix.mapreduce.Library;

public class L2 {

    public static class Join extends MapReduceBase
        implements Mapper<LongWritable, Text, Text, Text> {

        Set<String> hash;

        @Override
        public void configure(JobConf conf) {
            try {
                Path[] paths = DistributedCache.getLocalCacheFiles(conf);
                if (paths == null || paths.length < 1) {
                    throw new RuntimeException("DistributedCache no work.");
                }

                // Open the small table
                BufferedReader reader =
                    new BufferedReader(new InputStreamReader(new
                    FileInputStream(paths[0].toString())));
                String line;
                hash = new HashSet<String>(500);
                while ((line = reader.readLine()) != null) {
                    if (line.length() < 1) continue;
                    String[] fields = line.split("");
                    hash.add(fields[0]);
                }
            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
        }

        @Override
        public void map(
                LongWritable k,
                Text val,
                OutputCollector<Text, Text> oc,
                Reporter reporter) throws IOException {

            List<Text> fields = Library.splitLine(val, '');
            String name = fields.get(0).toString();

            String v;
            if (hash.contains(name)) {
                StringBuffer sb = new StringBuffer();
                sb.append(name);
                sb.append("");
                sb.append(fields.get(6).toString());
                oc.collect(null, new Text(sb.toString()));
            }

        }
    }

    public static void main(String[] args) throws IOException, URISyntaxException {

        JobConf lp = new JobConf(L2.class);
        lp.setJobName("Load Page Views");
        lp.setInputFormat(TextInputFormat.class);
        lp.setOutputKeyClass(Text.class);
        lp.setOutputValueClass(Text.class);
        lp.setMapperClass(Join.class);
        Properties props = System.getProperties();
        String dataDir = props.getProperty("PIGMIX_DIR", "/user/pig/tests/data/pigmix");
        for (Map.Entry<Object,Object> entry : props.entrySet()) {
            lp.set((String)entry.getKey(), (String)entry.getValue());
        }
        DistributedCache.addCacheFile(
            new URI(dataDir + "/power_users"), lp);
        FileInputFormat.addInputPath(lp, new Path(dataDir, "page_views"));
        FileOutputFormat.setOutputPath(lp, new Path("/user/"+System.getProperty("user.name")+"/L2out"));
        lp.setNumReduceTasks(0);
        Job loadPages = new Job(lp);

        JobControl jc = new JobControl("L2 join");
        jc.addJob(loadPages);

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
