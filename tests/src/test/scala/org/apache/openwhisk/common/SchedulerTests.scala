/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.openwhisk.common

import java.time.Instant

import scala.collection.mutable.Buffer
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.junit.JUnitRunner

import akka.actor.PoisonPill
import common.StreamLogging
import common.WskActorSystem

@RunWith(classOf[JUnitRunner])
class SchedulerTests extends FlatSpec with Matchers with WskActorSystem with StreamLogging {

  val timeBetweenCalls = 50 milliseconds
  val callsToProduce = 5
  val schedulerSlack = 100 milliseconds

  /**
   * Calculates the duration between two consecutive elements
   *
   * @param times the points in time to calculate the difference between
   * @return duration between each element of the given sequence
   */
  def calculateDifferences(times: Seq[Instant]) = {
    times sliding (2) map {
      case Seq(a, b) => Duration.fromNanos(java.time.Duration.between(a, b).toNanos)
    } toList
  }

  /**
   * Waits for the calls to be scheduled and executed. Adds one call-interval as additional slack
   */
  def waitForCalls(calls: Int = callsToProduce, interval: FiniteDuration = timeBetweenCalls) =
    Thread.sleep((calls + 1) * interval toMillis)

  private val retriesOnTestFailures = 5
  private val waitBeforeRetry = 1.second

  val behaviorwaitatleast = "A WaitAtLeast Scheduler"
  behavior of s"$behaviorwaitatleast"

  ignore should "be killable by sending it a poison pill" in {
    val testname = "be killable by sending it a poison pill"
    org.apache.openwhisk.utils
      .retry(
        {
          var callCount = 0
          val scheduled = Scheduler.scheduleWaitAtLeast(timeBetweenCalls) { () =>
            callCount += 1
            Future successful true
          }

          waitForCalls()
          // This is equal to a scheduled ! PoisonPill
          val shutdownTimeout = 10.seconds
          Await.result(akka.pattern.gracefulStop(scheduled, shutdownTimeout, PoisonPill), shutdownTimeout)

          val countAfterKill = callCount
          callCount should be >= callsToProduce

          waitForCalls()

          callCount shouldBe countAfterKill
        },
        retriesOnTestFailures,
        Some(waitBeforeRetry),
        Some(s"${this.getClass.getName} > $behaviorwaitatleast should $testname not successful, retrying.."))

  }

  it should "throw an expection when passed a negative duration" in {
    val testname = "throw an expection when passed a negative duration"
    org.apache.openwhisk.utils
      .retry(
        {
          an[IllegalArgumentException] should be thrownBy Scheduler.scheduleWaitAtLeast(-100 milliseconds) { () =>
            Future.successful(true)
          }
        },
        retriesOnTestFailures,
        Some(waitBeforeRetry),
        Some(s"${this.getClass.getName} > $behaviorwaitatleast should $testname not successful, retrying.."))

  }

  it should "wait at least the given interval between scheduled calls" in {
    val testname = "wait at least the given interval between scheduled calls"
    org.apache.openwhisk.utils
      .retry(
        {
          val calls = Buffer[Instant]()

          val scheduled = Scheduler.scheduleWaitAtLeast(timeBetweenCalls) { () =>
            calls += Instant.now
            Future successful true
          }

          waitForCalls()
          scheduled ! PoisonPill

          val differences = calculateDifferences(calls)
          withClue(s"expecting all $differences to be >= $timeBetweenCalls") {
            differences.forall(_ >= timeBetweenCalls)
          }
        },
        retriesOnTestFailures,
        Some(waitBeforeRetry),
        Some(s"${this.getClass.getName} > $behaviorwaitatleast should $testname not successful, retrying.."))

  }

  it should "stop the scheduler if an uncaught exception is thrown by the passed closure" in {
    val testname = "stop the scheduler if an uncaught exception is thrown by the passed closure"
    org.apache.openwhisk.utils
      .retry(
        {
          var callCount = 0
          val scheduled = Scheduler.scheduleWaitAtLeast(timeBetweenCalls) { () =>
            callCount += 1
            throw new Exception
          }

          waitForCalls()

          callCount shouldBe 1
        },
        retriesOnTestFailures,
        Some(waitBeforeRetry),
        Some(s"${this.getClass.getName} > $behaviorwaitatleast should $testname not successful, retrying.."))

  }

  it should "log scheduler halt message with tid" in {
    val testname = "log scheduler halt message with tid"
    org.apache.openwhisk.utils
      .retry(
        {
          implicit val transid = TransactionId.testing
          val msg = "test threw an exception"

          stream.reset()
          val scheduled = Scheduler.scheduleWaitAtLeast(timeBetweenCalls) { () =>
            throw new Exception(msg)
          }

          waitForCalls()
          stream.toString.split(" ").drop(1).mkString(" ") shouldBe {
            s"[ERROR] [$transid] [Scheduler] halted because $msg\n"
          }
        },
        retriesOnTestFailures,
        Some(waitBeforeRetry),
        Some(s"${this.getClass.getName} > $behaviorwaitatleast should $testname not successful, retrying.."))

  }

  it should "not stop the scheduler if the future from the closure is failed" in {
    val testname = "not stop the scheduler if the future from the closure is failed"
    org.apache.openwhisk.utils
      .retry(
        {
          var callCount = 0

          val scheduled = Scheduler.scheduleWaitAtLeast(timeBetweenCalls) { () =>
            callCount += 1
            Future failed new Exception
          }

          waitForCalls()
          scheduled ! PoisonPill

          callCount shouldBe callsToProduce
        },
        retriesOnTestFailures,
        Some(waitBeforeRetry),
        Some(s"${this.getClass.getName} > $behaviorwaitatleast should $testname not successful, retrying.."))

  }

  val behaviorwaitatmost = "A WaitAtMost Scheduler"
  "A WaitAtMost Scheduler" should "wait at most the given interval between scheduled calls" in {
    val testname = "wait at most the given interval between scheduled calls"
    org.apache.openwhisk.utils
      .retry(
        {
          val calls = Buffer[Instant]()
          val timeBetweenCalls = 200 milliseconds
          val computationTime = 100 milliseconds

          val scheduled = Scheduler.scheduleWaitAtMost(timeBetweenCalls) { () =>
            calls += Instant.now
            akka.pattern.after(computationTime, actorSystem.scheduler)(Future.successful(true))
          }

          waitForCalls(interval = timeBetweenCalls)
          scheduled ! PoisonPill

          val differences = calculateDifferences(calls)
          withClue(s"expecting all $differences to be <= $timeBetweenCalls") {
            differences should not be 'empty
            differences.forall(_ <= timeBetweenCalls + schedulerSlack)
          }
        },
        retriesOnTestFailures,
        Some(waitBeforeRetry),
        Some(s"${this.getClass.getName} > $behaviorwaitatmost should $testname not successful, retrying.."))

  }

  it should "delay initial schedule by given duration" in {
    val testname = "delay initial schedule by given duration"
    org.apache.openwhisk.utils
      .retry(
        {
          val timeBetweenCalls = 200 milliseconds
          val initialDelay = 1.second
          var callCount = 0

          val scheduled = Scheduler.scheduleWaitAtMost(timeBetweenCalls, initialDelay) { () =>
            callCount += 1
            Future successful true
          }

          try {
            Thread.sleep(initialDelay.toMillis)
            callCount should be <= 1

            Thread.sleep(2 * timeBetweenCalls.toMillis)
            callCount should be > 1
          } finally {
            scheduled ! PoisonPill
          }
        },
        retriesOnTestFailures,
        Some(waitBeforeRetry),
        Some(s"${this.getClass.getName} > $behaviorwaitatmost should $testname not successful, retrying.."))

  }

  it should "perform work immediately when requested" in {
    val testname = "perform work immediately when requested"
    org.apache.openwhisk.utils
      .retry(
        {
          val timeBetweenCalls = 200 milliseconds
          val initialDelay = 1.second
          var callCount = 0

          val scheduled = Scheduler.scheduleWaitAtMost(timeBetweenCalls, initialDelay) { () =>
            callCount += 1
            Future successful true
          }

          try {
            Thread.sleep(2 * timeBetweenCalls.toMillis)
            callCount should be(0)

            scheduled ! Scheduler.WorkOnceNow
            Thread.sleep(timeBetweenCalls.toMillis)
            callCount should be(1)
          } finally {
            scheduled ! PoisonPill
          }
        },
        retriesOnTestFailures,
        Some(waitBeforeRetry),
        Some(s"${this.getClass.getName} > $behaviorwaitatmost should $testname not successful, retrying.."))

  }

  it should "not wait when the closure takes longer than the interval" in {
    val testname = "not wait when the closure takes longer than the interval"
    org.apache.openwhisk.utils
      .retry(
        {
          val calls = Buffer[Instant]()
          val timeBetweenCalls = 200 milliseconds
          val computationTime = 300 milliseconds

          val scheduled = Scheduler.scheduleWaitAtMost(timeBetweenCalls) { () =>
            calls += Instant.now
            akka.pattern.after(computationTime, actorSystem.scheduler)(Future.successful(true))
          }

          waitForCalls(interval = timeBetweenCalls)
          scheduled ! PoisonPill

          val differences = calculateDifferences(calls)
          withClue(s"expecting all $differences to be <= $computationTime") {
            differences should not be 'empty
            differences.forall(_ <= computationTime + schedulerSlack)
          }
        },
        retriesOnTestFailures,
        Some(waitBeforeRetry),
        Some(s"${this.getClass.getName} > $behaviorwaitatmost should $testname not successful, retrying.."))

  }
}
