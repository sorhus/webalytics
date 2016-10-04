package com.github.sorhus.webalytics.model

case class Bucket(b: String)
case class Dimension(d: String)
case class Value(v: String)
case class ElementId(e: String)
case class DocumentId(d: Long)
case class Query(filter: Filter, buckets: List[Bucket], dimensions: List[Dimension])
case class Filter(f: List[List[Map[Bucket, Element]]])
case class DataPoint(elementId: ElementId, element: Element)
case class Space(s: Map[Dimension, List[Value]])

case class Element(e: Map[Dimension, List[Value]]) {
  def ++(that: Element) = {
    copy(e = e ++ that.e)
  }
}

object Element {
  def fromMap(data: Map[String, List[String]]): Element = {
    Element(data.map{case(d,v) => Dimension(d) -> v.map(Value.apply)})
  }

  def merge(dimensionValues: List[Element]): Element = {

    val grouped: Map[Dimension, List[(Dimension, List[Value])]] = dimensionValues
      .flatMap(_.e.toList)
      .groupBy{case(dimension,values) =>
        dimension
      }

    val e: Map[Dimension, List[Value]] = grouped.map{case(key, group) => // because mapValues does not serialize
      key -> group.flatMap{case(d, values) =>
        values
      }.distinct
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
                Dimension(d) -> v.map(Value.apply)
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

case class PostEvent(bucket: Bucket, documentId: DocumentId, element: Element)
case class PostMetaEvent(bucket: Bucket, element: Element)
case class PostEvent1(bucket: Bucket, elementId: ElementId, element: Element)
case class QueryEvent(query: Query, space: Element)
case class CloseBucket(b: Bucket)