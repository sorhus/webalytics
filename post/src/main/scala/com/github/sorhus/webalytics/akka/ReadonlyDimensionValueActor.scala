package com.github.sorhus.webalytics.akka

import akka.actor.{Actor, ActorRef, Props}
import com.github.sorhus.webalytics.model._
import org.slf4j.LoggerFactory


class ReadonlyDimensionValueActor(audienceActor: ActorRef) extends Actor {

  val log = LoggerFactory.getLogger(getClass)

  var state = DimensionValues()

  override def receive: Receive = {

    case e: PostMetaEvent =>
      log.info("received postmetaevent {}", e)
      state = state.update(e)

    case query: Query =>
      log.info("received query {}", query)
      val space = state.get(query.dimensions)
      log.info("space is: {}", space)
      audienceActor forward QueryEvent(query, space)

    case i: Initialize =>
      state.elems.foreach{case(bucket, space) =>
        audienceActor ! i.copy(space = Some(space))
      }

    case x =>
      println(s"readonly recieved $x")
  }
}

object ReadonlyDimensionValueActor {
  def props(audienceDao: ActorRef): Props = Props(new ReadonlyDimensionValueActor(audienceDao))
}
