package com.github.sorhus.webalytics.akka

import akka.actor.{ActorRef, Props}
import akka.persistence._
import com.github.sorhus.webalytics.model._
import org.roaringbitmap.RoaringBitmap
import com.github.sorhus.webalytics.impl.RoaringBitmapWrapper
import org.slf4j.LoggerFactory

import scala.util.Try
import scala.collection.mutable.{Map => MMap}

class BitsetAudienceActor(immutableActor: ActorRef = null) extends PersistentActor {

  val log = LoggerFactory.getLogger(getClass)

  var state = new BitsetState[RoaringBitmap](new RoaringBitmapWrapper().create _)

  override def persistenceId: String = "bitset-audience-actor"

  override def receiveRecover: Receive = {

    case e: PostEvent =>
//      log.info("received recover postevent {}", e)
      log.info("received recover postevent")
      state.post(e)

    case SnapshotOffer(_, snapshot: BitsetState[RoaringBitmap]) =>
      log.info("received recover snapshot {}", snapshot)
      state = snapshot

    case x =>
      log.info("received recover {}", x)
  }

  def handle(e: PostEvent): Unit = {
//    sender() ! Ack
    state.post(e)
  }

  override def receiveCommand: Receive = {

    case e: PostEvent =>
//      log.info("received postevent {}", e)
      log.info("received postevent")
//      persist(e)(handle)
//      persistAsync(e)(handle)
      handle(e)

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

    case CloseBucket(bucket) =>
      log.info("closing bucket")
      state remove bucket
      sender() ! Ack

    case SaveSnapshot =>
      log.info("saving snapshot")
      saveSnapshot(state)

    case cmd @ MakeImmutable(bucket, _) =>
      immutableActor forward cmd.copy(bitsets = state.bitsets(bucket))

    case Shutdown => sender() ! context.stop(self)

    case Debug => state.debug()

    case SaveSnapshotSuccess(metadata) =>
      log.info(s"snapshot saved. seqNum:${metadata.sequenceNr}, timeStamp:${metadata.timestamp}")

    case SaveSnapshotFailure(_, reason) =>
      log.info("failed to save snapshot: {}", reason)

    case x =>
      log.info(s"audience recieved {}", x)

  }

}

object BitsetAudienceActor {
  def props(immutableActor: ActorRef): Props = Props(new BitsetAudienceActor(immutableActor))
  def props(): Props = Props(new BitsetAudienceActor())
}


class BitsetState[T](newBitset: () => Bitset[T]) extends Serializable {

  val log = LoggerFactory.getLogger(getClass)

  private[webalytics] val bitsets = MMap[Bucket,MMap[Dimension,MMap[Value, Bitset[T]]]]()
  val staticBitSet: Bitset[T] = newBitset()

  def remove(bucket: Bucket) = {
    bitsets.remove(bucket)
  }

  def debug() = {
    bitsets.foreach{case (bucket, elements) =>
      elements.foreach{case (dimension, values) =>
        values.foreach{case(value, bitset) =>
          log.info(s"${bucket.b} ${dimension.d} ${value.v}: ${bitset.cardinality()}")
        }
      }
    }
  }

  def post(postEvent: PostEvent): Unit = post(postEvent.bucket, postEvent.documentId, postEvent.element)

  def post(bucket: Bucket, documentId: DocumentId, element: Element): Unit = {
    makeBitsetsExist(bucket, element)
    element.e.foreach{case (dimension, values) =>
      values.foreach{ value =>
        val bs = bitsets(bucket)(dimension)(value)
        bs.set(documentId.d, value = true)
      }
    }
  }

  def getCount(query: Query, dimensionValues: Map[Dimension, Set[Value]]): List[(Bucket, List[(Dimension, List[(Value, Long)])])] = {
    val audience = getAudience(query.filter)
    query.buckets.map{ bucket =>
      bucket -> dimensionValues.map{case(dimension, values) =>
        dimension -> values.map{value =>
          val bitset = Try(bitsets(bucket)(dimension)(value)).toOption
          value -> bitset.map(bs => staticBitSet.and(audience, bs).cardinality()).getOrElse(0L)
        }.toList
      }.toList
    }
  }

  private def makeBitsetsExist(bucket: Bucket, element: Element): Unit = {
    if(!bitsets.contains(bucket)) {
      bitsets.put(bucket, MMap[Dimension, MMap[Value, Bitset[T]]]())
    }
    element.e.foreach{ case (dimension, values) =>
      if(!bitsets(bucket).contains(dimension)) {
        bitsets(bucket).put(dimension, MMap[Value, Bitset[T]]())
      }
      values.foreach{ case(value) =>
        if(!bitsets(bucket)(dimension).contains(value)) {
          bitsets(bucket)(dimension).put(value, newBitset())
        }
      }
    }
  }

  private def getAudience(filter: Filter): Bitset[T] = {
    val ored: List[Bitset[T]] = filter.f.map{ and: List[Map[Bucket, Element]] =>
      val destination = newBitset()
      and.foreach{ or: Map[Bucket, Element] =>
        or.foreach{case(bucket, element) =>
          element.e.foreach{
            case(dimension, values) =>
              values.foreach { value =>
                Try(bitsets(bucket)(dimension)(value)).toOption.foreach(destination.or)
              }
          }
        }
      }
      destination
    }
    ored.tail.foreach(ored.head.and)
    val result = ored.head
    result
  }

}
