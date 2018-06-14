/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.benchmarks

import java.util.concurrent.ThreadLocalRandom

import akka.actor.ActorSystem
import akka.kafka.benchmarks.app.RunTestCommand
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}

import scala.io.StdIn

object BenchmarkApp extends App {

  implicit val sys = ActorSystem("Bench")
  implicit val mat = ActorMaterializer()

  def random(min: Int, max: Int): Int = {
    val mean = (max + min) / 2.0
    val stddev = (max - min) / 8.0
    math.round(math.min(math.max(ThreadLocalRandom.current.nextGaussian * stddev + mean, min), max)).toInt
  }

  def stream() =
    Source
      .fromIterator(() => Iterator.continually(random(min = 1, max = 10)))
      .to(Sink.ignore)
      .run()

  //stream()

  val kafkaHost = "localhost:9094"

  val conf = sys.settings.config.getConfig("cinnamon")
  println(conf.getConfig("prometheus"))

  Benchmarks.run(RunTestCommand("akka-plain-consumer", kafkaHost, 1000000))

  StdIn.readLine()

  sys.terminate()
}
