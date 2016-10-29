package com.github.sorhus.webalytics.akka.domain

import com.github.sorhus.webalytics.akka.model._

import scala.collection.concurrent.TrieMap
import scala.collection.mutable.{Map => MMap, Set => MSet}
import scala.collection.JavaConverters._

case class DomainState(data: Map[Bucket, Element] = Map[Bucket, Element]()) extends Serializable {

  def update(event: PostMetaEvent): DomainState = {
    if(data.contains(event.bucket) && data(event.bucket).contains(event.element)) {
      this
    } else {
      val merged = data.getOrElse(event.bucket, Element()) + event.element
      copy(data = data + (event.bucket -> merged))
    }
  }

  def get(bucket: Bucket): Element = data(bucket)

  def get(dimensions: List[Dimension]): Element = {
    dimensions match {
      case Dimension("*") :: Nil =>
        getAll
      case _ =>
        val filtered: Map[Dimension, Set[Value]] = getAll.e.filter{  case(d,v) =>
          dimensions.contains(d)
        }
        Element(filtered)
    }
  }

  def getAll: Element = Element.merge {
    data.map{ case(bucket, elements) =>
      elements
    }
  }

  def debug(): Unit = {
    println(data)
  }

}


case class MutableDomainState() extends Serializable {
  private val data: MMap[Dimension, MSet[Value]] = TrieMap[Dimension, MSet[Value]]()
  private val buckets = java.util.Collections.newSetFromMap[Bucket](new java.util.concurrent.ConcurrentHashMap[Bucket, java.lang.Boolean]()).asScala

  def update(event: PostMetaEvent): MutableDomainState = {
    if(!buckets.contains(event.bucket)) {
      buckets.add(event.bucket)
    }
    event.element.e.foreach{case(dimension, values) =>
      if(!data.contains(dimension)) {
          val set = java.util.Collections.newSetFromMap[Value](new java.util.concurrent.ConcurrentHashMap[Value, java.lang.Boolean]())
          values.foreach(set.add)
          data.put(dimension, set.asScala)
      } else {
        values.filterNot(data(dimension).contains)
          .foreach(data(dimension).add)
      }
    }
    this
  }

  def get(bucket: Bucket): Element = if(buckets.contains(bucket)) Element(data) else Element()

  def get(dimensions: List[Dimension]): Element = {
    dimensions match {
      case Dimension("*") :: Nil =>
        getAll
      case _ =>
        val filtered = getAll.e.filter{ case(d,v) =>
          dimensions.contains(d)
        }
        Element(filtered)
    }
  }

  def getAll: Element = buckets.headOption.map(get).getOrElse(Element())

  def debug(): Unit = {
    println(data)
  }

}
