package com.github.sorhus.webalytics.akka.domain

import akka.actor.{ActorRef, Props}
import akka.persistence._
import com.github.sorhus.webalytics.model._

class DomainActor(audienceActor: ActorRef, readOnlyMetaActor: Option[ActorRef]) extends TDomainActor {

  override def receiveCommand: Receive = {

    case e: PostMetaEvent =>
      log.debug("received postmetaevent")
//      persist(e)(handle)
//      persistAsync(e)(handle)
      handle(e)

    case query: Query =>
      log.info("received query {}", query)
      val space = state.get(query.dimensions)
      log.info("space is {}", space)
      audienceActor forward QueryEvent(query, space)

    case Getall =>
      sender() ! state.getAll

    case SaveSnapshot =>
      log.info("saving snapshot")
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

  override def receiveRecover: Receive = {

    case e: PostMetaEvent =>
      log.debug("received recover postmetaevent")
      handle(e)

    case SnapshotOffer(_, snapshot: State) =>
      log.info("restoring state from snapshot")
      state = snapshot

    case x =>
      log.info("received recover {}", x)

  }


}

object DomainActor {
  def props(audienceDao: ActorRef, readOnlyMetaActor: Option[ActorRef] = None): Props = {
    Props(new DomainActor(audienceDao, readOnlyMetaActor))
  }
}



