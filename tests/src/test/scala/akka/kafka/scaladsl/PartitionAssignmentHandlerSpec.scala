/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.scaladsl

import akka.actor.{Actor, ActorRef, Props}
import akka.pattern.ask
import akka.kafka._
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.scaladsl.OffsetStorage.{RequestOffset, StorePositions, TpsOffsets}
import akka.kafka.testkit.scaladsl.EmbeddedKafkaLike
import akka.stream.scaladsl.{Flow, Keep, Sink}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.util.JavaDurationConverters._
import akka.util.Timeout
import akka.{Done, NotUsed}
import net.manub.embeddedkafka.EmbeddedKafkaConfig
import org.apache.kafka.clients.consumer.{ConsumerRecord, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.scalatest._

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration._

object OffsetStorage {

  type TpOffsetMap = Map[TopicPartition, Long]

  final case class TpsOffsets(offsets: TpOffsetMap)

  sealed trait OffsetMessages
  final case class Clear() extends OffsetMessages
  final case class StoreHandledOffset(tp: TopicPartition, offset: Long) extends OffsetMessages
  final case class StorePositions(offsets: TpOffsetMap) extends OffsetMessages
  final case class RequestOffset(tps: Set[TopicPartition]) extends OffsetMessages
  final case class RequestAll() extends OffsetMessages

  class OffsetStorageActor extends Actor {

    def receive: Receive = store(Map.empty)

    private def store(current: TpOffsetMap): Receive = {
      case StorePositions(offsets) =>
        println(s"storing $offsets")
        sender() ! Done
        context.become(store(current ++ offsets))

      case StoreHandledOffset(tp, offset) =>
        println(s"storing $tp -> ${offset + 1}")
        sender() ! Done
        context.become(store(current.updated(tp, offset + 1L)))

      case RequestOffset(tps) =>
        val tpsWithOffsets = tps
          .filter(current.contains)
          .map { tp =>
            (tp, current(tp))
          }
          .toMap
        sender() ! TpsOffsets(tpsWithOffsets)

      case RequestAll() =>
        sender() ! TpsOffsets(current)

      case Clear() =>
        sender() ! Done
        context.become(store(Map.empty))
    }

  }
}

class PartitionAssignmentHandlerSpec
    extends SpecBase(kafkaPort = KafkaPorts.PartitionAssignmentHandlerSpec)
    with EmbeddedKafkaLike
    with Inside
    with OptionValues {

  implicit val patience: PatienceConfig = PatienceConfig(30.seconds, 500.millis)
  implicit val timeout: Timeout = 3.seconds

  override def createKafkaConfig: EmbeddedKafkaConfig =
    EmbeddedKafkaConfig(kafkaPort,
                        zooKeeperPort,
                        Map(
                          "num.partitions" -> "2",
                          "offsets.topic.replication.factor" -> "1"
                        ))
  final val Numbers0 = (1 to 20).map(_.toString + "-p0")
  final val Numbers1 = (1 to 20).map(_.toString + "-p1")
  final val partition1 = 1

  val positionTimeout: java.time.Duration = 10.seconds.asJava

  val businessLogic: Flow[ConsumerRecord[String, String], ConsumerRecord[String, String], NotUsed] =
    Flow[ConsumerRecord[String, String]]

  "External offsets" should {

    def seekOnAssign(offsetStoreActor: ActorRef): PartitionAssignmentHandler =
      new PartitionAssignmentHandler() {
        implicit val timeout: Timeout = 3.seconds

        override def onRevoke(revokedTps: Set[TopicPartition], consumer: RestrictedConsumer): Unit = {
          val offsets = revokedTps.map(tp => tp -> consumer.position(tp)).toMap
          offsetStoreActor ! StorePositions(offsets)
        }

        override def onAssign(assignedTps: Set[TopicPartition], consumer: RestrictedConsumer): Unit = {
          val eventualUnit = (offsetStoreActor ? RequestOffset(assignedTps)).mapTo[TpsOffsets]
          val tpsOffsets = Await.result(eventualUnit, 30.seconds)
          println(s"onAssign($assignedTps) got $tpsOffsets")

          tpsOffsets.offsets.foreach {
            case (tp, offset) =>
              println(s"seek($tp, ${offset + 1L})")
              consumer.seek(tp, offset + 1L)
          }

        }

        override def onStop(revokedTps: Set[TopicPartition], consumer: RestrictedConsumer): Unit =
          onRevoke(revokedTps, consumer)
      }

    "be updated from mapAsync" in assertAllStagesStopped {
      val groupId = createGroupId()
      val topic = createTopic(suffix = 0, partitions = 2)

      val consumerSettings = consumerDefaults
        .withGroupId(groupId)

      val offsetStoreActor: ActorRef =
        system.actorOf(Props(new OffsetStorage.OffsetStorageActor()), "offsetStorage")

      val subscription =
        Subscriptions.topics(topic).withPartitionAssignmentHandler(seekOnAssign(offsetStoreActor))

      val control = Consumer
        .plainSource(consumerSettings, subscription)
        .via(businessLogic)
        .mapAsync(1) { consumerRecord: ConsumerRecord[String, String] =>
          val tp = new TopicPartition(consumerRecord.topic(), consumerRecord.partition())
          val offset = consumerRecord.offset()
          (offsetStoreActor ? OffsetStorage.StoreHandledOffset(tp, offset)).map(_ => consumerRecord.value())
        }
        .toMat(Sink.seq)(Keep.both)
        .mapMaterializedValue(DrainingControl.apply)
        .run()

      awaitProduce(produceString(topic, Numbers0, partition0), produceString(topic, Numbers1, partition1))

      sleep(3.seconds, "to make it spin")

      control.drainAndShutdown().futureValue should contain theSameElementsAs (Numbers0 ++ Numbers1)

      val map = (offsetStoreActor ? OffsetStorage.RequestAll()).mapTo[TpsOffsets].futureValue

      map.offsets.get(new TopicPartition(topic, partition0)).value shouldBe Numbers0.size.toLong
      map.offsets.get(new TopicPartition(topic, partition1)).value shouldBe Numbers1.size.toLong

      (offsetStoreActor ? OffsetStorage.Clear()).mapTo[Done].futureValue shouldBe Done
    }
  }

  "Explicit committing" should {

    def commitOnRevoke(
        offsetStoreActor: ActorRef
    ): PartitionAssignmentHandler =
      new PartitionAssignmentHandler() {
        implicit val timeout: Timeout = 3.seconds

        override def onAssign(assignedTps: Set[TopicPartition], consumer: RestrictedConsumer): Unit = ()

        override def onRevoke(revokedTps: Set[TopicPartition], consumer: RestrictedConsumer): Unit = {
          println(s"onRevoke($revokedTps)")
          val eventualUnit = (offsetStoreActor ? RequestOffset(revokedTps)).mapTo[TpsOffsets]
          val tpsOffsets = Await.result(eventualUnit, 30.seconds)
          println(s"onRevoke($revokedTps) got $tpsOffsets")

          val asMap: Map[TopicPartition, OffsetAndMetadata] = tpsOffsets.offsets.mapValues(new OffsetAndMetadata(_))
          consumer.commitSync(asMap.asJava)
        }

        override def onStop(revokedTps: Set[TopicPartition], consumer: RestrictedConsumer): Unit =
          onRevoke(revokedTps, consumer)
      }

    "allow for less traffic to Kafka?" in assertAllStagesStopped {
      val groupId = createGroupId()
      val topic = createTopic(suffix = 0, partitions = 2)
      val initialConsume = 5

      val consumerSettings = consumerDefaults
        .withGroupId(groupId)

      val offsetStoreActor: ActorRef =
        system.actorOf(Props(new OffsetStorage.OffsetStorageActor()), "offsetStorage1")
      val subscription1 = Subscriptions.topics(topic).withPartitionAssignmentHandler(commitOnRevoke(offsetStoreActor))
      val subscription2 = Subscriptions.topics(topic).withPartitionAssignmentHandler(commitOnRevoke(offsetStoreActor))

      // TODO to make this useful, there must be the possibility for a scheduled commit

      val control1 = Consumer
        .plainSource(consumerSettings, subscription1)
        .take(initialConsume.toLong)
        .via(businessLogic)
        .mapAsync(1) { consumerRecord: ConsumerRecord[String, String] =>
          val tp = new TopicPartition(consumerRecord.topic(), consumerRecord.partition())
          val offset = consumerRecord.offset()
          (offsetStoreActor ? OffsetStorage.StoreHandledOffset(tp, offset)).map(_ => consumerRecord.value())
        }
        .toMat(Sink.seq)(Keep.both)
        .mapMaterializedValue(DrainingControl.apply)
        .run()

      awaitProduce(produceString(topic, Numbers0, partition0), produceString(topic, Numbers1, partition1))

      control1.streamCompletion.futureValue should have size 5
      sleep(3.seconds, "to make it spin")
      val offsets1_1 = (offsetStoreActor ? OffsetStorage.RequestAll()).mapTo[TpsOffsets].futureValue.offsets
      // either partition is read first and we took 5 elements
      val positionP0: Long = offsets1_1.getOrElse(new TopicPartition(topic, partition0), -1L)
      val positionP1: Long = offsets1_1.getOrElse(new TopicPartition(topic, partition1), -1L)
      positionP0 max positionP1 shouldBe initialConsume

      val control2 = Consumer
        .plainSource(consumerSettings, subscription2)
        .via(businessLogic)
        .mapAsync(1) { consumerRecord: ConsumerRecord[String, String] =>
          val tp = new TopicPartition(consumerRecord.topic(), consumerRecord.partition())
          val offset = consumerRecord.offset()
          (offsetStoreActor ? OffsetStorage.StoreHandledOffset(tp, offset)).map(_ => consumerRecord.value())
        }
        .toMat(Sink.seq)(Keep.both)
        .mapMaterializedValue(DrainingControl.apply)
        .run()

      sleep(5.seconds, "to make it spin")

      val received = control2
        .drainAndShutdown()
        .futureValue

      val expextInConsumer2 =
        if (positionP0 > -1) {
          Numbers0.drop(initialConsume) ++ Numbers1
        } else {
          Numbers0 ++ Numbers1.drop(initialConsume)
        }

      received should contain theSameElementsAs expextInConsumer2

      val offsets2 = (offsetStoreActor ? OffsetStorage.RequestAll()).mapTo[TpsOffsets].futureValue.offsets
      offsets2.get(new TopicPartition(topic, partition0)).value shouldBe Numbers0.size
      offsets2.get(new TopicPartition(topic, partition1)).value shouldBe Numbers1.size
    }

  }

  // Hit Kafka Timeout is hit in partitionHandler

  // How does an empty assign get handled (all partitions are balanced away)
}
