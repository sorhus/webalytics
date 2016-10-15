package com.github.sorhus.webalytics.akka.domain

import com.github.sorhus.webalytics.akka.model.{Bucket, Dimension, Element, PostMetaEvent}

case class DomainState(data: Map[Bucket, Element] = Map[Bucket, Element]()) extends Serializable {

  def update(event: PostMetaEvent): DomainState = {
    val merged = data.getOrElse(event.bucket, Element()) + event.element
    copy(data = data + (event.bucket -> merged))
  }

  def get(dimensions: List[Dimension]): Element = {
    dimensions match {
      case Dimension("*") :: Nil =>
        getAll
      case _ =>
        val filtered = getAll.e.filter{  case(d,v) =>
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
