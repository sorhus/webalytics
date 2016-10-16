package com.github.sorhus.webalytics.akka.segment

import akka.actor.{Actor, Props}
import akka.persistence.PersistentActor
import com.github.sorhus.webalytics.akka.model._
import org.slf4j.LoggerFactory


class ImmutableSegmentActor(path: String) extends PersistentActor {

  val log = LoggerFactory.getLogger(getClass)

  var state = new ImmutableSegmentState(path)

  override def persistenceId: String = "immutable-segment-actor"

  override def receiveCommand: Receive = {

    case QueryEvent(query: Query, space: Element) =>
      log.info("received query and space {}", (query, space))
      val response: Map[String, Map[String, Map[String, Long]]] = state.getCount(query, space.e)
        .map{case(bucket, dimensions) =>
          bucket.b -> dimensions.map{case(dimension, values) =>
            dimension.d -> values.map{case(value, count) =>
              value.v -> count
            }.toMap
          }.toMap
        }.toMap
      sender() ! response

    case l: LoadImmutable =>
      log.info("received LoadImmutable")
      persistAsync(l) { _ =>
        state.read(l.bucket, l.space.get)
        sender() ! Ack
      }

    case MakeImmutable(bucket, bitsets) =>
      this.state.write(bucket, bitsets)
      sender() ! Ack
  }

  override def receiveRecover: Receive = {
    case l: LoadImmutable =>
      log.info("received recover LoadImmutable")
      state.read(l.bucket, l.space.get)
  }
}

object ImmutableSegmentActor {
  def props(path: String) = Props(new ImmutableSegmentActor(path))
}
