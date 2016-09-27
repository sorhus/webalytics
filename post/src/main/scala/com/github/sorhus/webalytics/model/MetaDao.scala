package com.github.sorhus.webalytics.model

import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable

trait MetaDao {

  def addMeta(bucket: Bucket, element: Element):  Unit
  def getDocumentId(element_id: ElementId): Long
  def getDimensionValues(dimensions: List[Dimension]): List[(Dimension, List[Value])]

}

class CachedMetaDao(impl: MetaDao) extends MetaDao {

  import scalacache._
  import guava._
  import memoization._

  implicit val scalaCache = ScalaCache(GuavaCache())
  override def addMeta(bucket: Bucket, element: Element): Unit = {
    if(sync.get((bucket,element)).isEmpty) {
      impl.addMeta(bucket, element)
      sync.caching(bucket)(element)
    }
  }

  override def getDocumentId(element_id: ElementId): Long = impl.getDocumentId(element_id)

  override def getDimensionValues(dimensions: List[Dimension]): List[(Dimension, List[Value])] = memoizeSync {
    impl.getDimensionValues(dimensions)
  }
}

class DelayedBatchInsertMetaDao(impl: MetaDao) extends MetaDao {

  var id: Long = 0
  override def addMeta(bucket: Bucket, element: Element): Unit = ???
  override def getDocumentId(element_id: ElementId): Long = {
    id = id + 1L
    id
  }

  override def getDimensionValues(dimensions: List[Dimension]): List[(Dimension, List[Value])] = ???
}