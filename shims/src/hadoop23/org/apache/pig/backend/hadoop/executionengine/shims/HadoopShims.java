/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.backend.hadoop.executionengine.shims;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.DowngradeHelper;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TIPStatus;
import org.apache.hadoop.mapred.TaskReport;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.ContextFactory;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop23.PigJobControl;
import org.apache.hadoop.mapreduce.task.JobContextImpl;

public class HadoopShims {

    private static Log LOG = LogFactory.getLog(HadoopShims.class);
    private static Method getFileSystemClass;

    static public JobContext cloneJobContext(JobContext original) throws IOException, InterruptedException {

        Class clazz = null;
        try {
            clazz = Class.forName("org.apache.hadoop.mapreduce.ContextFactory");
        }
        catch (ClassNotFoundException e) {
            // we are most likely running on hadoop-2 in MRv1 mode
            JobContext newContext = new JobContextImpl(original.getConfiguration(), original.getJobID());
            return newContext;
        }
        JobContext newContext = ContextFactory.cloneContext(original,
                new JobConf(original.getConfiguration()));
        return newContext;
    }

    static public TaskAttemptContext createTaskAttemptContext(Configuration conf,
            TaskAttemptID taskId) {
        if (conf instanceof JobConf) {
            return new TaskAttemptContextImpl(new JobConf(conf), taskId);
        } else {
            return new TaskAttemptContextImpl(conf, taskId);
        }
    }

    static public JobContext createJobContext(Configuration conf,
            JobID jobId) {
        if (conf instanceof JobConf) {
            return new JobContextImpl(new JobConf(conf), jobId);
        } else {
            return new JobContextImpl(conf, jobId);
        }
    }

    static public boolean isMap(TaskAttemptID taskAttemptID) {
        TaskType type = taskAttemptID.getTaskType();
        if (type==TaskType.MAP)
            return true;

        return false;
    }

    static public TaskAttemptID getNewTaskAttemptID() {
        TaskAttemptID taskAttemptID = new TaskAttemptID("", 1, TaskType.MAP,
                1, 1);
        return taskAttemptID;
    }

    static public TaskAttemptID createTaskAttemptID(String jtIdentifier, int jobId, boolean isMap,
            int taskId, int id) {
        if (isMap) {
            return new TaskAttemptID(jtIdentifier, jobId, TaskType.MAP, taskId, id);
        } else {
            return new TaskAttemptID(jtIdentifier, jobId, TaskType.REDUCE, taskId, id);
        }
    }

    static public void storeSchemaForLocal(Job job, POStore st) {
        // Doing nothing for hadoop 23
    }

    static public String getFsCounterGroupName() {
        return "org.apache.hadoop.mapreduce.FileSystemCounter";
    }

    static public void commitOrCleanup(OutputCommitter oc, JobContext jc) throws IOException {
        oc.commitJob(jc);
    }

    public static JobControl newJobControl(String groupName, int timeToSleep) {
      return new PigJobControl(groupName, timeToSleep);
    }

    public static long getDefaultBlockSize(FileSystem fs, Path path) {
        return fs.getDefaultBlockSize(path);
    }

    public static Counters getCounters(Job job) throws Exception {
        Class clazz;
        try {
            clazz = Class.forName("org.apache.hadoop.mapreduce.Cluster");
        } catch(ClassNotFoundException e) {
            // we are most likely running on hadoop-2 in MRv1 mode
            return new Counters(job.getJob().getCounters());
        }

        try {
            //get constructor that takes a Configuration as argument
            Constructor constructor = clazz.getConstructor(new Class[]{Configuration.class});
            org.apache.hadoop.mapreduce.Cluster cluster = (org.apache.hadoop.mapreduce.Cluster)
                    constructor.newInstance(job.getJobConf());
            org.apache.hadoop.mapreduce.Job mrJob = cluster.getJob(job.getAssignedJobID());

            if (mrJob == null) { // In local mode, mrJob will be null
                mrJob = job.getJob();
            }
            return new Counters(mrJob.getCounters());
        } catch (Exception ir) {
            throw new IOException(ir);
        }
    }

    public static boolean isJobFailed(TaskReport report) {
        return report.getCurrentStatus()==TIPStatus.FAILED;
    }

    public static void unsetConf(Configuration conf, String key) {
        conf.unset(key);
    }

    /**
     * Fetch mode needs to explicitly set the task id which is otherwise done by Hadoop
     * @param conf
     * @param taskAttemptID
     */
    public static void setTaskAttemptId(Configuration conf, TaskAttemptID taskAttemptID) {
        conf.setInt("mapreduce.job.application.attempt.id", taskAttemptID.getId());
    }

    /**
     * Returns whether the give path has a FileSystem implementation.
     *
     * @param path path
     * @param conf configuration
     * @return true if the give path's scheme has a FileSystem implementation,
     *         false otherwise
     */
    public static boolean hasFileSystemImpl(Path path, Configuration conf) {
        String scheme = path.toUri().getScheme();
        if (scheme != null) {
            // Hadoop 0.23
            if (conf.get("fs.file.impl") != null) {
                String fsImpl = conf.get("fs." + scheme + ".impl");
                if (fsImpl == null) {
                    return false;
                }
            } else {
                // Hadoop 2.x HADOOP-7549
                if (getFileSystemClass == null) {
                    try {
                        getFileSystemClass = FileSystem.class.getDeclaredMethod(
                                "getFileSystemClass", String.class, Configuration.class);
                    } catch (NoSuchMethodException e) {
                        LOG.warn("Error while trying to determine if path " + path +
                                " has a filesystem implementation");
                        // Assume has implementation to be safe
                        return true;
                    }
                }
                try {
                    Object fs = getFileSystemClass.invoke(null, scheme, conf);
                    return fs == null ? false : true;
                } catch (Exception e) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Returns the progress of a Job j which is part of a submitted JobControl
     * object. The progress is for this Job. So it has to be scaled down by the
     * num of jobs that are present in the JobControl.
     *
     * @param j The Job for which progress is required
     * @return Returns the percentage progress of this Job
     * @throws IOException
     */
    public static double progressOfRunningJob(Job j)
            throws IOException {
        org.apache.hadoop.mapreduce.Job mrJob = j.getJob();
        try {
            return (mrJob.mapProgress() + mrJob.reduceProgress()) / 2;
        } catch (Exception ir) {
            return 0;
        }
    }

    public static void killJob(Job job) throws IOException {
        org.apache.hadoop.mapreduce.Job mrJob = job.getJob();
        try {
            if (mrJob != null) {
                mrJob.killJob();
            }
        } catch (Exception ir) {
            throw new IOException(ir);
        }
    }

    public static TaskReport[] getTaskReports(Job job, TaskType type) throws IOException {
        Method m = null;
        try {
            m = org.apache.hadoop.mapreduce.Job.class.getMethod("getTaskReports", TaskType.class);
        }
        catch (NoSuchMethodException e) {
            // we are most likely running on hadoop-2 in MRv1 mode
            JobClient jobClient = job.getJobClient();
            return (type == TaskType.MAP)
                    ? jobClient.getMapTaskReports(job.getAssignedJobID())
                    : jobClient.getReduceTaskReports(job.getAssignedJobID());
        }

        try {
            Class clazz = Class.forName("org.apache.hadoop.mapreduce.Cluster");
            //get constructor that takes a Configuration as argument
            Constructor constructor = clazz.getConstructor(new Class[]{Configuration.class});
            org.apache.hadoop.mapreduce.Cluster cluster = (org.apache.hadoop.mapreduce.Cluster)
                    constructor.newInstance(job.getJobConf());
            org.apache.hadoop.mapreduce.Job mrJob = cluster.getJob(job.getAssignedJobID());

            if (mrJob == null) { // In local mode, mrJob will be null
                mrJob = job.getJob();
            }
            //if we reached here, then we're running in hadoop-2 in MRv2 mode
            org.apache.hadoop.mapreduce.TaskReport[] reports = mrJob.getTaskReports(type);
            return DowngradeHelper.downgradeTaskReports(reports);
        } catch (Exception ir) {
            ir.printStackTrace();
            throw new IOException(ir);
        }
    }
}
