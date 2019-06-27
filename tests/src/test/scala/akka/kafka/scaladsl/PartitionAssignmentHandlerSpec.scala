/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.scaladsl

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import akka.actor.{Actor, ActorRef, Props}
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.pattern.ask
import akka.kafka._
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.scaladsl.OffsetStorage.{Clear, RequestOffset, StorePositions, TpsOffsets}
import akka.kafka.testkit.scaladsl.TestcontainersKafkaLike
import akka.stream.Supervision.Stop
import akka.stream.scaladsl.{Flow, Keep, Sink}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.util.JavaDurationConverters._
import akka.util.Timeout
import akka.{Done, NotUsed}
import org.apache.kafka.clients.consumer.{ConsumerRecord, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.scalatest._

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

object OffsetStorage {

  type TpOffsetMap = Map[TopicPartition, OffsetAndMetadata]

  final case class TpsOffsets(offsets: TpOffsetMap)

  sealed trait OffsetMessages
  final case class Clear() extends OffsetMessages
  final case class StoreHandledOffset(tp: TopicPartition, offset: Long, metadata: String) extends OffsetMessages
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

      case StoreHandledOffset(tp, offset, metadata) =>
        println(s"storing $tp -> ${offset + 1}")
        sender() ! Done
        context.become(store(current.updated(tp, new OffsetAndMetadata(offset + 1L, metadata))))

      case RequestOffset(tps) =>
        val tpsWithOffsets = tps
          .filter(current.contains)
          .map { tp =>
            (tp, current(tp))
          }
          .toMap
        sender() ! TpsOffsets(tpsWithOffsets)
        context.become(store(current -- tps))

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
    with TestcontainersKafkaLike
    with Inside
    with OptionValues {

  implicit val patience: PatienceConfig = PatienceConfig(30.seconds, 500.millis)
  implicit val timeout: Timeout = 3.seconds

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
          val offsets = revokedTps.map(tp => tp -> new OffsetAndMetadata(consumer.position(tp), "")).toMap
          offsetStoreActor ! StorePositions(offsets)
        }

        override def onAssign(assignedTps: Set[TopicPartition], consumer: RestrictedConsumer): Unit = {
          val eventualUnit = (offsetStoreActor ? RequestOffset(assignedTps)).mapTo[TpsOffsets]
          val tpsOffsets = Await.result(eventualUnit, 30.seconds)
          println(s"onAssign($assignedTps) got $tpsOffsets")

          tpsOffsets.offsets.foreach {
            case (tp, offsetMeta) =>
              val position = offsetMeta.offset + 1L
              println(s"seek($tp, ${position})")
              consumer.seek(tp, position)
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
          (offsetStoreActor ? OffsetStorage.StoreHandledOffset(tp, offset, "")).map(_ => consumerRecord.value())
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
          val availableOffsets = (offsetStoreActor ? RequestOffset(revokedTps)).mapTo[TpsOffsets]
          val tpsOffsets = Await.result(availableOffsets, 30.seconds)
          println(s"onRevoke($revokedTps) got $tpsOffsets")

          val asMap: Map[TopicPartition, OffsetAndMetadata] = tpsOffsets.offsets
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
          (offsetStoreActor ? OffsetStorage.StoreHandledOffset(tp, offset, "")).map(_ => consumerRecord.value())
        }
        .toMat(Sink.seq)(Keep.both)
        .mapMaterializedValue(DrainingControl.apply)
        .run()

      awaitProduce(produceString(topic, Numbers0, partition0), produceString(topic, Numbers1, partition1))

      control1.streamCompletion.futureValue should have size 5
      sleep(3.seconds, "to make it spin")
      val offsets1_1 = (offsetStoreActor ? OffsetStorage.RequestAll()).mapTo[TpsOffsets].futureValue.offsets
      // either partition is read first and we took 5 elements
      val positionP0: Long = offsets1_1.mapValues(_.offset).getOrElse(new TopicPartition(topic, partition0), -1L)
      val positionP1: Long = offsets1_1.mapValues(_.offset).getOrElse(new TopicPartition(topic, partition1), -1L)
      positionP0 max positionP1 shouldBe initialConsume

      val control2 = Consumer
        .plainSource(consumerSettings, subscription2)
        .via(businessLogic)
        .mapAsync(1) { consumerRecord: ConsumerRecord[String, String] =>
          val tp = new TopicPartition(consumerRecord.topic(), consumerRecord.partition())
          val offset = consumerRecord.offset()
          (offsetStoreActor ? OffsetStorage.StoreHandledOffset(tp, offset, "")).map(_ => consumerRecord.value())
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

  /*
   * These tests compare the use of partitioned sources and the partition assignment handler for managing
   * resources connected to the assigned partitions.
   */
  "starting and stopping resources" should {

    class DummyActor(topicPartition: TopicPartition) extends Actor {
      override def receive: Receive = {
        case Stop =>
          log.debug("stopping actor for {}", topicPartition)
          context.stop(self)
          sender() ! Done

        case msg: String =>
          sender() ! Done
      }
    }

    "work with partitioned sources" in {
      val groupId = createGroupId()
      val maxPartitions = 2
      val topic = createTopic(suffix = 0, maxPartitions)

      val consumerSettings = consumerDefaults.withStopTimeout(0.seconds).withGroupId(groupId)
      val subscription = Subscriptions.topics(topic)

      @volatile var resources = Map.empty[TopicPartition, ActorRef]

      val control1 = Consumer
        .plainPartitionedSource(consumerSettings, subscription)
        .mapAsyncUnordered(maxPartitions) {
          case (topicPartition, source) =>
            val resource = system.actorOf(Props(new DummyActor(topicPartition)))
            resources = resources + (topicPartition -> resource)
            source
              .mapAsync(1) { record =>
                (resource ? record.value()).map(_ => record)
              }
              .runWith(Sink.ignore)
              .flatMap(_ => resource ? Stop)
              .map { _ =>
                log.debug("removing {}", topicPartition)
                resources = resources - topicPartition
              }
        }
        .toMat(Sink.ignore)(Keep.both)
        .mapMaterializedValue(DrainingControl.apply)
        .run()

      awaitProduce(produceString(topic, Numbers0, partition0), produceString(topic, Numbers1, partition1))

      sleep(3.seconds, "to make it spin")

      control1.drainAndShutdown().futureValue
      resources shouldBe Symbol("empty")
    }

    "work with a handler" in {
      val groupId = createGroupId()
      val maxPartitions = 2
      val topic = createTopic(suffix = 0, maxPartitions)

      val consumerSettings = consumerDefaults.withStopTimeout(0.seconds).withGroupId(groupId)

      var resources = Map.empty[TopicPartition, ActorRef]

      val handler = new PartitionAssignmentHandler {
        override def onRevoke(revokedTps: Set[TopicPartition], consumer: RestrictedConsumer): Unit = {
          println(s"onRevoke $revokedTps")
          revokedTps.foreach { tp =>
            resources(tp) ! Stop
            resources = resources - tp
          }
        }
        override def onAssign(assignedTps: Set[TopicPartition], consumer: RestrictedConsumer): Unit = {
          println(s"onAssign $assignedTps")
          assignedTps.foreach { tp =>
            val resource = system.actorOf(Props(new DummyActor(tp)))
            resources = resources + (tp -> resource)
          }
        }
        override def onStop(revokedTps: Set[TopicPartition], consumer: RestrictedConsumer): Unit = {
          println(s"onStop $revokedTps")
          revokedTps.foreach { tp =>
            resources(tp) ! Stop
            resources = resources - tp
          }
        }
      }
      val subscription = Subscriptions.topics(topic).withPartitionAssignmentHandler(handler)

      val control1 = Consumer
        .plainSource(consumerSettings, subscription)
        .mapAsyncUnordered(maxPartitions) { record =>
          val resource = resources(new TopicPartition(record.topic(), record.partition()))
          (resource ? record.value()).map(_ => record)
        }
        .toMat(Sink.ignore)(Keep.both)
        .mapMaterializedValue(DrainingControl.apply)
        .run()

      awaitProduce(produceString(topic, Numbers0, partition0), produceString(topic, Numbers1, partition1))

      sleep(3.seconds, "to make it spin")

      control1.drainAndShutdown().futureValue
      resources shouldBe Symbol("empty")
    }
  }

  /*
   * The use case for this test is to cancel processing of messages for a revoked partition that are in
   * the stream already. This doesn't give idempotence, but gets closer.
   */
  "cancel processing of messages from revoked partitions" should {

    class CurrentPartitions {
      var currentlyAssigned = Set.empty[TopicPartition]

      /** Kafka has signalled these partitions are revoked, but some may be re-assigned just after revoking. */
      var partitionsToRevoke: Set[TopicPartition] = Set.empty

      def onRevoke(revokedTps: Set[TopicPartition]): Unit =
        partitionsToRevoke ++= revokedTps

      def onAssign(assignedTps: Set[TopicPartition]): Unit = {
        log.debug(s"onAssign $assignedTps")
        currentlyAssigned = currentlyAssigned -- partitionsToRevoke ++ assignedTps
      }
    }

    "work" in {
      val groupId = createGroupId()
      val maxPartitions = 2
      val topic = createTopic(suffix = 0, maxPartitions)

      val rebalanceFinished = new AtomicBoolean(false)

      var store1 = List.empty[String]

      def storeResult1 = { tup: (CommittableMessage[String, String], Int) =>
        store1 = tup._1.record.value :: store1
        // store result
        Future.successful(tup)
      }

      val currentPartitions = new CurrentPartitions

      val handler1 = new PartitionAssignmentHandler {
        override def onRevoke(revokedTps: Set[TopicPartition], consumer: RestrictedConsumer): Unit =
          currentPartitions.onRevoke(revokedTps)
        override def onAssign(assignedTps: Set[TopicPartition], consumer: RestrictedConsumer): Unit = {
          val partitions = assignedTps.map(_.partition)
          log.debug(s"onAssign $partitions")
          currentPartitions.onAssign(assignedTps)
          if (assignedTps.size == 1) {
            log.debug("rebalanceFinished")
            rebalanceFinished.set(true)
          }
        }
        override def onStop(revokedTps: Set[TopicPartition], consumer: RestrictedConsumer): Unit =
          onRevoke(revokedTps, consumer)
      }

      val consumerSettings = consumerDefaults.withStopTimeout(0.seconds).withGroupId(groupId)
      val subscription1 = Subscriptions.topics(topic).withPartitionAssignmentHandler(handler1)
      val control1 = Consumer
        .committableSource(consumerSettings, subscription1)
        .mapAsync(maxPartitions)(lenthyOperation(rebalanceFinished))
        .filter {
          case (cm, _) =>
            val keep =
              currentPartitions.currentlyAssigned.contains(new TopicPartition(cm.record.topic, cm.record.partition))
            log.debug(
              s"$keep  ${cm.record.value} partition=${cm.record.partition()} ${currentPartitions.currentlyAssigned}"
            )
            keep
        }
        // store the result
        .mapAsync(maxPartitions)(storeResult1)
        .map {
          case (cm, _) =>
            cm.committableOffset
        }
        .toMat(Committer.sink(committerDefaults.withMaxBatch(1)))(Keep.both)
        .mapMaterializedValue(DrainingControl.apply)
        .run()

      awaitProduce(produceString(topic, Numbers0, partition0), produceString(topic, Numbers1, partition1))

      val subscription2 = Subscriptions.topics(topic)
      val control2 = Consumer
        .committableSource(consumerSettings, subscription2)
        .map(_.committableOffset)
        .toMat(Committer.sink(committerDefaults))(Keep.both)
        .mapMaterializedValue(DrainingControl.apply)
        .run()

      eventually {
        store1 should contain allElementsOf Numbers0
      }
      store1.diff(Numbers0).size shouldBe <(Numbers1.size)
      control1.drainAndShutdown().futureValue shouldBe Done
      control2.drainAndShutdown().futureValue shouldBe Done
    }

    def lenthyOperation(finish: AtomicBoolean) = { cm: CommittableMessage[String, String] =>
      Thread.sleep(500)
      // lengthy operation on the data
      val result = 42
      Future.successful((cm, result))
    }

  }

  // Other questions/ideas

  // Kafka Timeout is hit in partitionHandler

  // How does an empty assign get handled (all partitions are balanced away)

  // Any way to create a Committer that would support this

}
