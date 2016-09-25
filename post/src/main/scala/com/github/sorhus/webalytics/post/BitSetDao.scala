package com.github.sorhus.webalytics.post

import com.zaxxer.sparsebits.SparseBitSet
import scala.collection.mutable
import scala.util.Try

class BitSetDao extends AudienceDao {

  private val bitsets = mutable.Map[Bucket, mutable.Map[Dimension, mutable.Map[Value, SparseBitSet]]]()

  def debug = {
    bitsets.foreach{ case (bucket, elements) =>
      elements.foreach{case (dimension, values) =>
        values.foreach{case(value, bitset) =>
          println(s"${bucket.b} ${dimension.d} ${value.v}: ${bitset.cardinality()}")
        }
      }
    }
  }

  override def post(bucket: Bucket, element_id: ElementId, element: Element)(implicit metaDao: MetaDao): Unit = {
    metaDao.addMeta(bucket, element)
    val bitsets: Map[Bucket, Map[Dimension, Map[Value, SparseBitSet]]] = getBitSets(bucket, element)
    val documentId = metaDao.getDocumentId(element_id)
    element.e.foreach{case (dimension, values) =>
      values.foreach{ value =>
        metaDao.addMeta(bucket, element)
        bitsets(bucket)(dimension)(value).set(documentId.toInt, true)
      }
    }
  }

  override def getCount(query: Query)(implicit metaDao: MetaDao): List[(Bucket, List[(Dimension, List[(Value, Long)])])] = {
    val bitsets: Map[Bucket, Map[Dimension, Map[Value, SparseBitSet]]] = getBitSets(query.filter)
    val audience = getAudience(bitsets, query.filter)(metaDao)
    val dimensionValues: List[(Dimension, Seq[Value])] = metaDao.getDimensionValues(query.dimensions)
    query.buckets.map{ bucket =>
      bucket -> dimensionValues.map{case(dimension, values) =>
        dimension -> values.map{value =>
          val bitset = Try(bitsets(bucket)(dimension)(value)).toOption
          value -> bitset.map(bs => SparseBitSet.and(audience, bs).cardinality()).getOrElse(0).toLong
        }.toList
      }
    }
  }

  private def getBitSets(filter: Filter): Map[Bucket, Map[Dimension, Map[Value, SparseBitSet]]] = {
    val all: Map[Bucket, Element] = filter.f
      .flatMap(_.flatten)
      .groupBy{case(bucket, elements) =>
        bucket
      }.map{case(bucket, allElements: List[(Bucket, Element)]) =>
        bucket -> allElements.map(_._2).reduce(_ ++ _)
      }

    val res: Map[Bucket, Map[Dimension, Map[Value, SparseBitSet]]] = all.map{case(bucket, elements) =>
      if(!bitsets.contains(bucket))
        bitsets.put(bucket, mutable.Map[Dimension, mutable.Map[Value, SparseBitSet]]())
      bucket -> elements.e.map{ case (dimension, values) =>
        if(!bitsets(bucket).contains(dimension))
          bitsets(bucket).put(dimension, mutable.Map[Value, SparseBitSet]())
        dimension -> values.map{ case(value) =>
          if(!bitsets(bucket)(dimension).contains(value))
            bitsets(bucket)(dimension).put(value, new SparseBitSet())
          value -> bitsets(bucket)(dimension)(value)
        }.toMap
      }
    }
    res
  }

  private def getBitSets(bucket: Bucket, element: Element): Map[Bucket, Map[Dimension, Map[Value, SparseBitSet]]] = {
    getBitSets(Filter(List(List(Map(bucket -> element)))))
  }

  private def getAudience(bitsets: Map[Bucket, Map[Dimension, Map[Value, SparseBitSet]]], filter: Filter)(implicit metaDao: MetaDao): SparseBitSet = {
    val ored: List[SparseBitSet] = filter.f.map{ and: List[Map[Bucket, Element]] =>
      val destination = new SparseBitSet()
      and.foreach{ or: Map[Bucket, Element] =>
        or.foreach{case(bucket, element) =>
          element.e.foreach{
            case(dimension, Value("*") :: Nil) =>
              val values = metaDao.getDimensionValues(dimension :: Nil).flatMap(_._2)
              values.foreach { value =>
                destination.or(bitsets(bucket)(dimension)(value))
              }
            case(dimension, values) =>
              values.foreach { value =>
                destination.or(bitsets(bucket)(dimension)(value))
              }
          }
        }
      }
      destination
    }
    ored.tail.foreach(ored.head.and)
    ored.head
  }

}
