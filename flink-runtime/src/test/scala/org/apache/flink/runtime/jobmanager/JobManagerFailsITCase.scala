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

package org.apache.flink.runtime.jobmanager

import akka.actor._
import akka.testkit.{ImplicitSender, TestKit}
import org.apache.flink.runtime.jobgraph.{AbstractJobVertex, JobGraph}
import org.apache.flink.runtime.jobmanager.Tasks.{NoOpInvokable, BlockingNoOpInvokable}
import org.apache.flink.runtime.messages.JobManagerMessages._
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.{TaskManagerRegistered,
NotifyWhenTaskManagerRegistered, ThrowException}
import org.apache.flink.runtime.testingUtils.TestingTaskManagerMessages.{JobManagerTerminated,
NotifyWhenJobManagerTerminated}
import org.apache.flink.runtime.testingUtils.TestingUtils
import org.junit.runner.RunWith
import org.scalatest.{WordSpecLike, Matchers, BeforeAndAfterAll}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class JobManagerFailsITCase(_system: ActorSystem) extends TestKit(_system) with ImplicitSender
with WordSpecLike with Matchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("TestingActorSystem", TestingUtils.testConfig))

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "The TaskManager" should {
    "detect a lost connection to the JobManager and try to reconnect to it" in {
      val num_slots = 11

      val cluster = TestingUtils.startTestingClusterDeathWatch(num_slots, 1)

      val tm = cluster.getTaskManagers(0)
      val jm = cluster.getJobManager

      try{
        jm ! RequestNumberRegisteredTaskManager
        expectMsg(1)

        tm ! NotifyWhenJobManagerTerminated(jm)

        jm ! PoisonPill

        expectMsgClass(classOf[JobManagerTerminated])

        cluster.restartJobManager()

        cluster.waitForTaskManagersToBeRegistered()

        cluster.getJobManager ! RequestNumberRegisteredTaskManager

        expectMsg(1)
      } finally {
        cluster.stop()
      }
    }
  }

  "The system" should {
    "recover from an exception by aborting all running jobs" in {
      val num_slots = 20

      val sender = new AbstractJobVertex("BlockingSender")
      sender.setParallelism(num_slots)
      sender.setInvokableClass(classOf[BlockingNoOpInvokable])
      val jobGraph = new JobGraph("Blocking Testjob", sender)

      val noOp = new AbstractJobVertex("NoOpInvokable")
      noOp.setParallelism(num_slots)
      noOp.setInvokableClass(classOf[NoOpInvokable])
      val jobGraph2 = new JobGraph("NoOp Testjob", noOp)

      val cluster = TestingUtils.startTestingClusterDeathWatch(num_slots/2, 2)

      val jm = cluster.getJobManager

      try{
        within(TestingUtils.TESTING_DURATION) {
          jm ! SubmitJob(jobGraph)
          expectMsg(SubmissionSuccess(jobGraph.getJobID))

          jm ! ThrowException("Test exception")

          // Check if the JobManager is still alive
          jm ! Identify(1)

          expectMsgType[ActorIdentity]

          jm ! NotifyWhenTaskManagerRegistered(2)

          expectMsg(TaskManagerRegistered(2))

          jm ! SubmitJob(jobGraph2)
          val response = expectMsgType[SubmissionResponse]

          response match {
            case SubmissionSuccess(jobID) => jobID should equal(jobGraph2.getJobID)
            case SubmissionFailure(jobID, t) =>
              fail("Submission of the second job failed.", t)
          }

          val result = expectMsgType[JobResultSuccess]

          result.jobID should equal(jobGraph2.getJobID)
        }
      } finally {
        cluster.stop()
      }
    }
  }
}
