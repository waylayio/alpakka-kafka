/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka

// TODO is this the place for this?
final case class SubStreamContext[C](initialOffset: Long, context: C)
