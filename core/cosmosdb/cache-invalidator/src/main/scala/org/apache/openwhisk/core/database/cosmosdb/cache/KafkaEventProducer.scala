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

package org.apache.openwhisk.core.database.cosmosdb.cache

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.scaladsl.Producer
import akka.kafka.{ProducerMessage, ProducerSettings}
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{OverflowStrategy, QueueOfferResult}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.openwhisk.connector.kafka.KamonMetricsReporter

import scala.collection.immutable.Seq
import scala.concurrent.{ExecutionContext, Future, Promise}

case class KafkaEventProducer(
  settings: ProducerSettings[String, String],
  topic: String,
  eventProducerConfig: EventProducerConfig)(implicit system: ActorSystem)
    extends EventProducer {
  private implicit val executionContext: ExecutionContext = system.dispatcher

  private val queue = Source
    .queue[(Seq[String], Promise[Done])](eventProducerConfig.bufferSize, OverflowStrategy.dropNew) //TODO Use backpressure
    .map {
      case (msgs, p) =>
        ProducerMessage.multi(msgs.map(newRecord), p)
    }
    .via(Producer.flexiFlow(producerSettings))
    .map {
      case ProducerMessage.MultiResult(_, passThrough) =>
        passThrough.success(Done)
      case _ => //As we use multi mode only other modes need not be handled
    }
    .toMat(Sink.ignore)(Keep.left)
    .run

  override def send(msg: Seq[String]): Future[Done] = {
    val promise = Promise[Done]
    queue.offer(msg -> promise).flatMap {
      case QueueOfferResult.Enqueued    => promise.future
      case QueueOfferResult.Dropped     => Future.failed(new Exception("Kafka request queue is full."))
      case QueueOfferResult.QueueClosed => Future.failed(new Exception("Kafka request queue was closed."))
      case QueueOfferResult.Failure(f)  => Future.failed(f)
    }
  }

  def close(): Future[Done] = {
    queue.complete()
    queue.watchCompletion()
  }

  private def newRecord(msg: String) = new ProducerRecord[String, String](topic, "messages", msg)

  private def producerSettings =
    settings.withProperty(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, KamonMetricsReporter.name)
}
