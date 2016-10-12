package com.github.sorhus.webalytics.akka.segment

import com.github.sorhus.webalytics.impl.RoaringBitmapWrapper
import com.github.sorhus.webalytics.model._
import org.roaringbitmap.{FastAggregation, RoaringBitmap}
import org.slf4j.LoggerFactory

import scala.collection.mutable.{Map => MMap}
import scala.util.Try

class MutableState extends Serializable {

  val log = LoggerFactory.getLogger(getClass)

  private[webalytics] val bitsets = MMap[Bucket,MMap[Dimension,MMap[Value, Bitset[RoaringBitmap]]]]()

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
          value -> bitset.map(bs => RoaringBitmap.and(audience.impl(), bs.impl()).getLongCardinality).getOrElse(0L)
        }.toList
      }.toList
    }
  }

  private def makeBitsetsExist(bucket: Bucket, element: Element): Unit = {
    if(!bitsets.contains(bucket)) {
      bitsets.put(bucket, MMap[Dimension, MMap[Value, Bitset[RoaringBitmap]]]())
    }
    element.e.foreach{ case (dimension, values) =>
      if(!bitsets(bucket).contains(dimension)) {
        bitsets(bucket).put(dimension, MMap[Value, Bitset[RoaringBitmap]]())
      }
      values.foreach{ case(value) =>
        if(!bitsets(bucket)(dimension).contains(value)) {
          bitsets(bucket)(dimension).put(value, new RoaringBitmapWrapper())
        }
      }
    }
  }

  private def getAudience(filter: Filter): Bitset[RoaringBitmap] = {
    val toAnd: List[RoaringBitmap] = filter.f.map{ and: List[Map[Bucket, Element]] =>
      val toOr: List[Bitset[RoaringBitmap]] = and.flatMap{ or: Map[Bucket, Element] =>
        or.flatMap{case(bucket, element) =>
          element.e.flatMap{
            case(dimension, values) =>
              values.flatMap { value =>
                Try(bitsets(bucket)(dimension)(value)).toOption
              }
          }
        }
      }
      FastAggregation.or(toOr.map(_.impl()): _*)
    }
    val result = FastAggregation.and(toAnd: _*)
    new RoaringBitmapWrapper(result)
  }

}
