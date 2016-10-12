package com.github.sorhus.webalytics.akka.meta

import akka.actor.{ActorRef, Props}
import akka.persistence._
import com.github.sorhus.webalytics.model._

class MetaDataActor(audienceActor: ActorRef, readOnlyMetaActor: Option[ActorRef]) extends TMetaDataActor {

  override def receiveCommand: Receive = {

    case e: PostMetaEvent =>
//      log.info("received {}", e)
      log.info("received postmetaevent")
//      persist(e)(handle)
//      persistAsync(e)(handle)
      handle(e)
      readOnlyMetaActor.foreach(_ ! e)

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
      log.info(s"received $x")

  }

}

object MetaDataActor {
  def props(audienceDao: ActorRef, readOnlyMetaActor: Option[ActorRef] = None): Props = {
    Props(new MetaDataActor(audienceDao, readOnlyMetaActor))
  }
}



