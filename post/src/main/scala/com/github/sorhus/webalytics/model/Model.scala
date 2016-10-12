package com.github.sorhus.webalytics.model

import java.util.UUID

//sealed trait Model with AnyVal

case class Bucket(b: String) //extends AnyVal
case class Dimension(d: String) //extends AnyVal
case class Value(v: String) //extends AnyVal
case class ElementId(e: String = UUID.randomUUID().toString) //extends AnyVal
case class DocumentId(d: Long) //extends AnyVal

case class Query(filter: Filter, buckets: List[Bucket], dimensions: List[Dimension], immutable: Boolean = false)
case class Filter(f: List[List[Map[Bucket, Element]]])
case class DataPoint(elementId: ElementId, element: Element)
case class Space(s: Map[Dimension, List[Value]])

case class Element(e: Map[Dimension, Set[Value]]) {
  def +(that: Element) = {
    val keys = e.keys ++ that.e.keys
    val merged = keys.map{key =>
      key -> (e.getOrElse(key, Set.empty) ++ that.e.getOrElse(key, Set.empty))
    }.toMap
    copy(e = merged)
  }
}

object Element {

  def apply() = new Element(Map.empty)
  def fromMap(data: Map[String, Set[String]]): Element = {
    Element(data.map{case(d,v) => Dimension(d) -> v.map(Value.apply)})
  }

  def merge(dimensionValues: Iterable[Element]): Element = {

    val grouped: Map[Dimension, Iterable[(Dimension, Set[Value])]] = dimensionValues
      .flatMap(_.e.toList)
      .groupBy{case(dimension,values) =>
        dimension
      }

    val e: Map[Dimension, Set[Value]] = grouped.map{case(key, group) => // because mapValues does not serialize
      key -> group.flatMap{case(d, values) =>
        values
      }.toSet
    }

    Element(e)
  }
}

case class JsonQuery(
  filter: List[List[Map[String, Map[String,List[String]]]]],
  buckets: List[String],
  dimensions: List[String]
) {
  def toQuery = {

    Query(
      filter = Filter(
        filter.map{ and =>
          and.map{ or =>
            or.map{case(b, e) =>
              Bucket(b) -> Element(e.map{case(d, v) =>
                Dimension(d) -> v.map(Value.apply).toSet
              })
            }
          }
        }
      ),
      buckets = buckets.map(Bucket.apply),
      dimensions = dimensions.map(Dimension.apply)
    )
  }
}

object JsonResult {
  def fromResult(result: List[(Bucket, List[(Dimension, List[(Value, Long)])])]): List[(String, List[(String, List[(String, Long)])])] = {
    result.map{case(bucket, dimensions) =>
      bucket.b -> dimensions.map{case(dimension, values) =>
        dimension.d -> values.map{case(value, count) =>
          value.v -> count
        }
      }
    }
  }
}

case class PostEvent(bucket: Bucket, elementId: ElementId, documentId: DocumentId, element: Element)
case class PostMetaEvent(bucket: Bucket, element: Element)
case class PostCommand(bucket: Bucket, elementId: ElementId, element: Element)
case class QueryEvent(query: Query, space: Element)
case class CloseBucket(b: Bucket)