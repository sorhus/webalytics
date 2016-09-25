package com.github.sorhus.webalytics.post

case class Bucket(x: String)
case class Dimension(x: String)
case class Value(x: String)
case class ElementId(x: String)
case class DocumentId(x: Long)
case class Query(filter: Filter, buckets: List[Bucket], dimensions: List[Dimension])
case class Filter(f: List[List[Map[Bucket, Element]]])

case class Element(e: Map[Dimension,List[Value]]) {
  def ++(that: Element) = {
    copy(e = e ++ that.e)
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