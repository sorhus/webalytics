package com.github.sorhus.webalytics.model

import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

trait MetaDao {

  def addMeta(bucket: Bucket, element: Element): Future[Any]
  def getDocumentId(element_id: ElementId): Long
  def getDimensionValues(dimensions: List[Dimension]): List[(Dimension, List[Value])]

}

class CachedMetaDao(impl: MetaDao) extends MetaDao {

  import scalacache._
  import guava._
  import memoization._

  implicit val scalaCache = ScalaCache(GuavaCache())
  override def addMeta(bucket: Bucket, element: Element) = {
    if(sync.get((bucket,element)).isEmpty) {
      val res = impl.addMeta(bucket, element)
      sync.caching(bucket)(element)
      res
    } else {
      Future.successful(true)
    }
  }

  override def getDocumentId(element_id: ElementId): Long = impl.getDocumentId(element_id)

  override def getDimensionValues(dimensions: List[Dimension]): List[(Dimension, List[Value])] = memoizeSync {
    impl.getDimensionValues(dimensions)
  }
}

class DelayedBatchInsertMetaDao(impl: MetaDao) extends MetaDao {

  var id: Long = 0
  val meta = mutable.Map[Bucket, Element]()

  override def addMeta(bucket: Bucket, element: Element) = {
    meta.put(bucket, Element.merge(meta.getOrElse(bucket, Element(Map())) :: element :: Nil))
    Future.successful(true)
  }

  def commit() = meta.foreach{case(bucket, element) =>
    println(s"adding metadata ${bucket}: ${element}")
    val res = Await.result(impl.addMeta(bucket, element), Duration.Inf)
    println(res)
  }

  override def getDocumentId(element_id: ElementId): Long = {
    id = id + 1L
    id
  }

  override def getDimensionValues(dimensions: List[Dimension]): List[(Dimension, List[Value])] = {
    impl.getDimensionValues(dimensions)
  }
}