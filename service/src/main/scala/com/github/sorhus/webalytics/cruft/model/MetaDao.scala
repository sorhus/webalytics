package com.github.sorhus.webalytics.cruft.model

import com.github.sorhus.webalytics.akka.event._
import com.github.sorhus.webalytics.model._
import com.github.sorhus.webalytics.cruft.redis.RedisMetaDao

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

trait MetaDao {

  def addMeta(bucket: Bucket, element: Element): Future[Any]
  def getDocumentId(element_id: ElementId): Long
  def getDimensionValues(dimensions: List[Dimension]): List[(Dimension, Set[Value])]
  def time(name: String)(f: Nothing) = {
  }
}

class DelayedBatchInsertMetaDao(impl: RedisMetaDao)(implicit context: ExecutionContext) extends MetaDao {

  var id: Long = 0
  val metaBuckets = mutable.Map[Bucket, Element]()
  val metaDocumentIds = mutable.Map[String, Long]()


  override def addMeta(bucket: Bucket, element: Element) = Future {
    metaBuckets.put(bucket, Element.merge(metaBuckets.getOrElse(bucket, Element.fromMap(Map())) :: element :: Nil))
  }

  def commit() = {
    val futures = impl.batchInsertDocumentIds(metaDocumentIds.toMap).toList :::
      metaBuckets.toList.map{case(bucket, element)  =>
        impl.addMeta(bucket, element)
      }
    Future.sequence(futures)
  }

  override def getDocumentId(element_id: ElementId): Long = {
    id = id + 1L
    metaDocumentIds.put(element_id.e, id)
    id
  }

  override def getDimensionValues(dimensions: List[Dimension]): List[(Dimension, Set[Value])] = {
    impl.getDimensionValues(dimensions)
  }

}





class DevNullMetaDao extends MetaDao {
  override def addMeta(bucket: Bucket, element: Element): Future[Any] = Future.successful("")
  override def getDocumentId(element_id: ElementId): Long = -1L
  override def getDimensionValues(dimensions: List[Dimension]): List[(Dimension, Set[Value])] = Nil
}

