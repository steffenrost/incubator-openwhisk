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

package org.apache.openwhisk.core.controller.test

import akka.actor.ActorRefFactory
import akka.testkit.TestProbe

import org.apache.openwhisk.core.entitlement._

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import org.apache.openwhisk.core.entity.UserLimits
import org.apache.openwhisk.core.loadBalancer.ShardingContainerPoolBalancer

import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer

import common.StreamLogging

import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.junit.runner.RunWith
import org.scalamock.scalatest.MockFactory
import org.scalatest.junit.JUnitRunner
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import scala.concurrent.Future
import org.apache.openwhisk.common.Logging
import org.apache.openwhisk.common.TransactionId
import org.apache.openwhisk.core.WhiskConfig
import org.apache.openwhisk.core.connector.ActivationMessage
import org.apache.openwhisk.core.connector.Message
import org.apache.openwhisk.core.connector.MessageConsumer
import org.apache.openwhisk.core.connector.MessageProducer
import org.apache.openwhisk.core.connector.MessagingProvider
import org.apache.openwhisk.core.entity.ControllerInstanceId
import org.apache.openwhisk.core.entity.ExecManifest
import org.apache.openwhisk.core.entity.InvokerInstanceId
import org.apache.openwhisk.core.entity.ByteSize
import org.apache.openwhisk.core.entity.test.ExecHelpers
import org.apache.openwhisk.core.loadBalancer.FeedFactory
import org.apache.openwhisk.core.loadBalancer.InvokerPoolFactory

/**
 * Tests rate throttle.
 *
 * @Idioglossia
 * "using Specification DSL to write unit tests, as in should, must, not, be"
 */
@RunWith(classOf[JUnitRunner])
class RateThrottleTests extends FlatSpec with Matchers with StreamLogging with ExecHelpers with MockFactory {

  implicit val transid = TransactionId.testing
  val subject = WhiskAuthHelpers.newIdentity()

  behavior of "Rate Throttle"

  //val loadBalancer = new LoadBalancer
  implicit val am = ActorMaterializer()
  val config = new WhiskConfig(ExecManifest.requiredProperties)
  def mockMessaging(): MessagingProvider = {
    val messaging = stub[MessagingProvider]
    val producer = stub[MessageProducer]
    val consumer = stub[MessageConsumer]
    (messaging
      .getProducer(_: WhiskConfig, _: Option[ByteSize])(_: Logging, _: ActorSystem))
      .when(*, *, *, *)
      .returns(producer)
    (messaging
      .getConsumer(_: WhiskConfig, _: String, _: String, _: Int, _: FiniteDuration)(_: Logging, _: ActorSystem))
      .when(*, *, *, *, *, *, *)
      .returns(consumer)
    (producer
      .send(_: String, _: Message, _: Int))
      .when(*, *, *)
      .returns(Future.successful(new RecordMetadata(new TopicPartition("fake", 0), 0, 0, 0l, 0l, 0, 0)))

    messaging
  }
  val feedProbe = new FeedFactory {
    def createFeed(f: ActorRefFactory, m: MessagingProvider, p: (Array[Byte]) => Future[Unit]) =
      TestProbe().testActor
  }
  val invokerPoolProbe = new InvokerPoolFactory {
    override def createInvokerPool(
      actorRefFactory: ActorRefFactory,
      messagingProvider: MessagingProvider,
      messagingProducer: MessageProducer,
      sendActivationToInvoker: (MessageProducer, ActivationMessage, InvokerInstanceId) => Future[RecordMetadata],
      monitor: Option[ActorRef]): ActorRef =
      TestProbe().testActor
  }
  val loadBalancer =
    new ShardingContainerPoolBalancer(config, ControllerInstanceId("0"), feedProbe, invokerPoolProbe, mockMessaging)

  it should "throttle when rate exceeds allowed threshold" in {
    new RateThrottler(loadBalancer, "test", _ => 0).check(subject).ok shouldBe false
    val rt = new RateThrottler(loadBalancer, "test", _ => 1)
    rt.check(subject).ok shouldBe true
    rt.check(subject).ok shouldBe false
    rt.check(subject).ok shouldBe false
    Thread.sleep(1.minute.toMillis)
    rt.check(subject).ok shouldBe true
  }

  it should "check against an alternative limit if passed in" in {
    val withLimits = subject.copy(limits = UserLimits(invocationsPerMinute = Some(5)))
    val rt = new RateThrottler(loadBalancer, "test", u => u.limits.invocationsPerMinute.getOrElse(1))
    rt.check(withLimits).ok shouldBe true // 1
    rt.check(withLimits).ok shouldBe true // 2
    rt.check(withLimits).ok shouldBe true // 3
    rt.check(withLimits).ok shouldBe true // 4
    rt.check(withLimits).ok shouldBe true // 5
    rt.check(withLimits).ok shouldBe false
  }

}
