package com.github.sorhus.webalytics.akka.meta

import akka.actor.{ActorRef, Props}
import akka.persistence._
import com.github.sorhus.webalytics.model._

class MetaDataActor(audienceActor: ActorRef) extends TMetaDataActor {

  override def receiveCommand: Receive = {

    case e: PostMetaEvent =>
      log.info("received {}", e)
      persist(e)(handle)

    case query: Query =>
      log.info("received query {}", query)
      val space = state.get(query.dimensions)
      log.info("space is {}", space)
      audienceActor forward QueryEvent(query, space)

    case Getall =>
      sender() ! state.getAll

    case SaveSnapshot =>
      log.info("saving snapshot: {}", state)
      saveSnapshot(state)

    case Shutdown =>
      sender() ! context.stop(self)

    case Debug =>
      state.debug()

    case SaveSnapshotSuccess(metadata) =>
      log.info(s"snapshot saved. seqNum:${metadata.sequenceNr}, timeStamp:${metadata.timestamp}")

    case SaveSnapshotFailure(_, reason) =>
      log.info("failed to save snapshot", reason)

    case x =>
      log.info(s"recieved $x")

  }

}

object MetaDataActor {
  def props(audienceDao: ActorRef): Props = Props(new MetaDataActor(audienceDao))
}



