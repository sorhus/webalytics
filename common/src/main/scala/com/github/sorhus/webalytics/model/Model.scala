package com.github.sorhus.webalytics.akka.model

import java.util.UUID

import scala.collection.mutable.{Map => MMap, Set => MSet}
sealed trait Model extends Serializable

case class Bucket(b: String) extends AnyVal
case class Dimension(d: String) extends AnyVal
case class DocumentId(d: Long) extends AnyVal
case class ElementId(e: String = UUID.randomUUID().toString) extends AnyVal
case class Filter(f: List[List[Map[Bucket, Element]]]) extends Model
case class Value(v: String) extends AnyVal
case class Query(filter: Filter, buckets: List[Bucket], dimensions: List[Dimension], immutable: Boolean = false) extends Model {
}

case object Root {
  val value = Value("root")
  val dimension = Dimension("root")
  val element: Element = Element(Map(dimension -> Set(value)))
  def filter(bucket: Bucket) = Filter(List(Map(bucket -> element) :: Nil))
  def query(bucket: Bucket) = Query(filter(bucket), bucket :: Nil, Root.dimension :: Nil)

}

//trait Element extends Model {
//  def +(element: Element) = ???
//
//  def e: Map[Dimension, Set[Value]]
//}

case class Element(e: Map[Dimension, Set[Value]]) extends Model {

  def contains(element: Element): Boolean = {
    element.e.forall{case(dimension, values) =>
      e.contains(dimension) && values.forall(e(dimension).contains)
    }
  }

  def +(that: Element): Element = {
    val keys = e.keys ++ that.e.keys
    val merged = keys.map{key =>
      key -> (e.getOrElse(key, Set.empty) ++ that.e.getOrElse(key, Set.empty))
    }.toMap
    copy(e = merged)
  }

}

//case class MutableElement(e: MMap[Dimension, MSet[Value]]) extends Model {
//
//  def contains(element: Element): Boolean = {
//    element.e.forall{case(dimension, values) =>
//      e.contains(dimension) && values.forall(e(dimension).contains)
//    }
//    false
//  }
//
//  def +(that: Element): Element = {
//    that.e.foreach{case (dimension, values)  =>
//      if(!e.contains(dimension)) {
//        e.put(dimension, MSet(values.toSeq: _*))
//      }
//      values.foreach{value =>
//        if(!e(dimension).contains(value)) {
//          e(dimension).add(value)
//        }
//      }
//    }
//    null
//  }
//
//  def toElement: Element = null
//    Element {
//    e.map{case(dimension, values) =>
//      dimension -> values.toSet
//    }.toMap
//  }
//
//}

//object MutableElement {
//  def apply(e: Element): MutableElement = null
//  {
//    val map = TrieMap[Dimension, MSet[Value]]()
//    e.e.foreach{case(dimension, values) =>
//        map.put(dimension, MSet(values.toSeq: _*))
//    }
//    MutableElement(map)
//  }
//}

object Element {
  def apply(data: MMap[Dimension, MSet[Value]]): Element = Element {
    data.map{case(dimension,values) =>
      dimension -> values.toSet
    }.toMap
  }

  def apply() = new Element(Map.empty)

  def fromMap(data: Map[String, Set[String]]): Element = {
    Element(data.map{case(d,v) => Dimension(d) -> v.map(Value.apply)})
  }

  def toMap(e: Element): Map[String, Set[String]] = {
    e.e.map{case(d,v) => d.d -> v.map(_.v)}
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

