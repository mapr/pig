/**
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
package org.apache.pig.backend.hadoop20;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob.State;

/**
 * extends the hadoop JobControl to remove the hardcoded sleep(5000)
 * as most of this is private we have to use reflection
 * 
 * See {@link https://svn.apache.org/repos/asf/hadoop/common/branches/branch-0.20/src/mapred/org/apache/hadoop/mapred/jobcontrol/JobControl.java}
 *
 */
public class PigJobControl extends JobControl {
  private static final Log log = LogFactory.getLog(PigJobControl.class);

  private static Field runnerState;
    
  private static Method checkRunningJobs;
  private static Method checkWaitingJobs;
  private static Method startReadyJobs;


  private static boolean initSuccesful;

  static {
    try {

      runnerState = org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl.class.getDeclaredField("runnerState");
      runnerState.setAccessible(true);
      checkRunningJobs = org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl.class.getDeclaredMethod("checkRunningJobs");
      checkRunningJobs.setAccessible(true);
      checkWaitingJobs = org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl.class.getDeclaredMethod("checkWaitingJobs");
      checkWaitingJobs.setAccessible(true);
      startReadyJobs = org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl.class.getDeclaredMethod("startReadyJobs");
      startReadyJobs.setAccessible(true);

      initSuccesful = true;
    } catch (Exception e) {
      log.warn("falling back to default JobControl (not using hadoop 0.20 ?)", e);
      initSuccesful = false;
    }
  }

  protected int timeToSleep;

  /**
   * Construct a job control for a group of jobs.
   * @param groupName a name identifying this group
   * @param pigContext
   * @param conf
   */
  public PigJobControl(String groupName, int timeToSleep) {
    super(groupName);
    this.timeToSleep = timeToSleep;
  }

  public int getTimeToSleep() {
    return timeToSleep;
  }

  public void setTimeToSleep(int timeToSleep) {
    this.timeToSleep = timeToSleep;
  }

  private void setRunnerState(ThreadState state) {
    try {
      runnerState.set(this, state);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }


  private ThreadState getRunnerState() {
    try {
      return (ThreadState)runnerState.get(this);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }



  /**
   *  The main loop for the thread.
   *  The loop does the following:
   *    Check the states of the running jobs
   *    Update the states of waiting jobs
   *    Submit the jobs in ready state
   */
  public void run() {
    if (!initSuccesful) {
      super.run();
      return;
    }
    
    setRunnerState(ThreadState.RUNNING);
    while (true) {
      while (getRunnerState() == ThreadState.SUSPENDED) {
        try {
          Thread.sleep(timeToSleep);
        } catch (Exception e) {
          //TODO the thread was interrupted, do something!!!
        }
      }
      mainLoopAction();
      if (getRunnerState() != ThreadState.RUNNING &&
          getRunnerState() != ThreadState.SUSPENDED) {
        break;
      }
      try {
        Thread.sleep(timeToSleep);
      }
      catch (Exception e) {

      }
      if (getRunnerState() != ThreadState.RUNNING &&
          getRunnerState() != ThreadState.SUSPENDED) {
        break;
      }
    }
    setRunnerState(ThreadState.STOPPED);
  }

  private void mainLoopAction() {
    try {
      checkRunningJobs.invoke(this);
      checkWaitingJobs.invoke(this);
      startReadyJobs.invoke(this);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
