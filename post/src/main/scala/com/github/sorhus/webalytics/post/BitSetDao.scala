package com.github.sorhus.webalytics.post

import com.zaxxer.sparsebits.SparseBitSet
import scala.util.Try
import scala.collection.mutable.{Map => MMap}

case class BitSet(bs: SparseBitSet) {
  def set(bit: Long, value: Boolean): Unit = bs.set(bit.toInt, value)
  def and(that: BitSet): Unit = bs.and(that.bs)
  def or(that: BitSet): Unit = bs.or(that.bs)
  def cardinality(): Long = bs.cardinality()
}

object BitSet {
  def apply() = new BitSet(new SparseBitSet())
  def and(bitSet1: BitSet, bitset2: BitSet): BitSet = BitSet(SparseBitSet.and(bitSet1.bs, bitset2.bs))
}

class BitSetDao extends AudienceDao {

  private val bitsets = MMap[Bucket,MMap[Dimension,MMap[Value, BitSet]]]()

  def debug() = {
    bitsets.foreach{case (bucket, elements) =>
      elements.foreach{case (dimension, values) =>
        values.foreach{case(value, bitset) =>
          println(s"${bucket.b} ${dimension.d} ${value.v}: ${bitset.cardinality()}")
        }
      }
    }
  }

  override def post(bucket: Bucket, element_id: ElementId, element: Element)(implicit metaDao: MetaDao): Unit = synchronized {
    metaDao.addMeta(bucket, element)

    val bitsets: Map[Bucket, Map[Dimension, Map[Value, BitSet]]] = getBitSets(bucket, element)
    val documentId = metaDao.getDocumentId(element_id)
    element.e.foreach{case (dimension, values) =>
      values.foreach{ value =>
        metaDao.addMeta(bucket, element)
        val bs = bitsets(bucket)(dimension)(value)
        this.synchronized(bs.set(documentId.toInt, value = true))
      }
    }
  }

  override def getCount(query: Query)(implicit metaDao: MetaDao): List[(Bucket, List[(Dimension, List[(Value, Long)])])] = {
    val bitsets: Map[Bucket, Map[Dimension, Map[Value, BitSet]]] = getBitSets(query)
    val audience = getAudience(bitsets, query.filter)(metaDao)
    val dimensionValues: List[(Dimension, Seq[Value])] = metaDao.getDimensionValues(query.dimensions)
    query.buckets.map{ bucket =>
      bucket -> dimensionValues.map{case(dimension, values) =>
        dimension -> values.map{value =>
          val bitset = Try(bitsets(bucket)(dimension)(value)).toOption
          value -> bitset.map(bs => BitSet.and(audience, bs).cardinality()).getOrElse(0L)
        }.toList
      }
    }
  }

  private def getBitSets(query: Query)(implicit metaDao: MetaDao): Map[Bucket, Map[Dimension, Map[Value, BitSet]]] = {
    val buckets: Set[Bucket] = query.filter.f.flatMap(_.flatten).map(_._1).toSet ++ query.buckets.toSet
    val dimensionValues: List[Element] = Element(metaDao.getDimensionValues(query.dimensions).toMap) :: query.filter.f.flatMap(_.flatten).map(_._2)
    val all: Map[Bucket, Element] = buckets.map{ bucket =>
      val element = Element.merge(dimensionValues)
      bucket -> element
    }.toMap
    getBitSets(all)
  }

  private def getBitSets(all: Map[Bucket, Element]): Map[Bucket, Map[Dimension, Map[Value, BitSet]]] = this.synchronized {
    val res: Map[Bucket, Map[Dimension, Map[Value, BitSet]]] = all.map{case(bucket, elements) =>
      if(!bitsets.contains(bucket)) {
        bitsets.put(bucket, MMap[Dimension, MMap[Value, BitSet]]())
      }
      bucket -> elements.e.map{ case (dimension, values) =>
        if(!bitsets(bucket).contains(dimension)) {
          bitsets(bucket).put(dimension, MMap[Value, BitSet]())
        }
        dimension -> values.map{ case(value) =>
          if(!bitsets(bucket)(dimension).contains(value)) {
            bitsets(bucket)(dimension).put(value, BitSet())
          }
          value -> bitsets(bucket)(dimension)(value)
        }.toMap
      }
    }
    res
  }

  private def getBitSets(bucket: Bucket, element: Element): Map[Bucket, Map[Dimension, Map[Value, BitSet]]] = {
    getBitSets(Map(bucket -> element))
  }

  private def getAudience(bitsets: Map[Bucket, Map[Dimension, Map[Value, BitSet]]], filter: Filter)(implicit metaDao: MetaDao): BitSet = {
    val ored: List[BitSet] = filter.f.map{ and: List[Map[Bucket, Element]] =>
      val destination = BitSet()
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
