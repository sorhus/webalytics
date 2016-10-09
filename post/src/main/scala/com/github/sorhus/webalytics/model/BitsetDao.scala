package com.github.sorhus.webalytics.model

import com.github.sorhus.webalytics.impl.ImmutableRoaringBitmapWrapper
import org.roaringbitmap.buffer.{ImmutableRoaringBitmap, MutableRoaringBitmap}

import scala.collection.immutable.Seq
import scala.collection.mutable.{Map => MMap}
import scala.util.Try
import collection.JavaConverters._

class BitsetDao[T](newBitset: () => Bitset[T]) extends AudienceDao {

  private[webalytics] val bitsets = MMap[Bucket,MMap[Dimension,MMap[Value, Bitset[T]]]]()
  val staticBitSet: Bitset[T] = newBitset()

  def debug() = {
    bitsets.foreach{case (bucket, elements) =>
      elements.foreach{case (dimension, values) =>
        values.foreach{case(value, bitset) =>
          println(s"${bucket.b} ${dimension.d} ${value.v}: ${bitset.cardinality()}")
        }
      }
    }
  }

  override def post(bucket: Bucket, element_id: ElementId, element: Element)(implicit metaDao: MetaDao): Unit = {
    metaDao.addMeta(bucket, element)
    val bitsets: Map[Bucket, Map[Dimension, Map[Value, Bitset[T]]]] = getBitSets(bucket, element)
    val documentId = metaDao.getDocumentId(element_id)
    element.e.foreach{case (dimension, values) =>
      values.foreach{ value =>
        val bs = bitsets(bucket)(dimension)(value)
        bs.synchronized(bs.set(documentId.toInt, value = true))
      }
    }
  }

  override def getCount(query: Query)(implicit metaDao: MetaDao): List[(Bucket, List[(Dimension, List[(Value, Long)])])] = {
    val bitsets: Map[Bucket, Map[Dimension, Map[Value, Bitset[T]]]] = getBitSets(query)
    val audience = getAudience(bitsets, query.filter)(metaDao)
    val dimensionValues: List[(Dimension, Set[Value])] = metaDao.getDimensionValues(query.dimensions)
    query.buckets.map{ bucket =>
      bucket -> dimensionValues.map{case(dimension, values) =>
        dimension -> values.map{value =>
          val bitset = Try(bitsets(bucket)(dimension)(value)).toOption
          value -> bitset.map(bs => staticBitSet.and(audience, bs).cardinality()).getOrElse(0L)
        }.toList
      }
    }
  }

  private def getBitSets(query: Query)(implicit metaDao: MetaDao): Map[Bucket, Map[Dimension, Map[Value, Bitset[T]]]] = {
    val buckets: Set[Bucket] = query.filter.f.flatMap(_.flatten).map(_._1).toSet ++ query.buckets.toSet
    val dimensionValues: List[Element] = Element(metaDao.getDimensionValues(query.dimensions).toMap) :: query.filter.f.flatMap(_.flatten).map(_._2)
    val all: Map[Bucket, Element] = buckets.map{ bucket =>
      val element = Element.merge(dimensionValues)
      bucket -> element
    }.toMap
    getBitSets(all)
  }

  private def getBitSets(all: Map[Bucket, Element]): Map[Bucket, Map[Dimension, Map[Value, Bitset[T]]]] = {
    val res: Map[Bucket, Map[Dimension, Map[Value, Bitset[T]]]] = all.map{case(bucket, elements) =>
      if(!bitsets.contains(bucket)) {
        this.synchronized {
          if(!bitsets.contains(bucket))
            bitsets.put(bucket, MMap[Dimension, MMap[Value, Bitset[T]]]())
        }
      }
      bucket -> elements.e.map{ case (dimension, values) =>
        if(!bitsets(bucket).contains(dimension)) {
          this.synchronized {
            if (!bitsets(bucket).contains(dimension))
              bitsets(bucket).put(dimension, MMap[Value, Bitset[T]]())
          }
        }
        dimension -> values.map{ case(value) =>
          if(!bitsets(bucket)(dimension).contains(value)) {
            this.synchronized {
              if (!bitsets(bucket)(dimension).contains(value))
                bitsets(bucket)(dimension).put(value, newBitset())
            }
          }
          value -> bitsets(bucket)(dimension)(value)
        }.toMap
      }
    }
    res
  }

  private def getBitSets(bucket: Bucket, element: Element): Map[Bucket, Map[Dimension, Map[Value, Bitset[T]]]] = {
    getBitSets(Map(bucket -> element))
  }

  private def getAudience(bitsets: Map[Bucket, Map[Dimension, Map[Value, Bitset[T]]]], filter: Filter)(implicit metaDao: MetaDao): Bitset[T] = {
    val ored: List[Bitset[T]] = filter.f.map{ and: List[Map[Bucket, Element]] =>
      val destination = newBitset()
      and.foreach{ or: Map[Bucket, Element] =>
        or.foreach{case(bucket, element) =>
          element.e.foreach{
//            case(dimension, Value("*") :: Nil) =>
//              val values = metaDao.getDimensionValues(dimension :: Nil).flatMap(_._2)
//              values.foreach { value =>
//                destination.or(bitsets(bucket)(dimension)(value))
//              }
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

class ImmutableBitsetDao(bitsets: Map[Bucket, Map[Dimension, Map[Value, Bitset[ImmutableRoaringBitmap]]]])
  extends AudienceDao {

  override def post(bucket: Bucket, element_id: ElementId, element: Element)(implicit metaDao: MetaDao): Unit = ???

  override def getCount(query: Query)(implicit metaDao: MetaDao): List[(Bucket, List[(Dimension, List[(Value, Long)])])] = {
    val audience = getAudience(bitsets, query.filter)(metaDao)
    val dimensionValues: List[(Dimension, Set[Value])] = metaDao.getDimensionValues(query.dimensions)
    query.buckets.map{ bucket =>
      bucket -> dimensionValues.map{case(dimension, values) =>
        dimension -> values.map{value =>
          val bitset = Try(bitsets(bucket)(dimension)(value)).toOption
          bitset.foreach(bs => println(s"andCard: ${ImmutableRoaringBitmap.andCardinality(audience.impl(), bs.impl())}"))
          bitset.foreach(bs => println(s"and.card: ${ImmutableRoaringBitmap.and(audience.impl(), bs.impl()).getCardinality}"))
          value -> bitset.map(bs => ImmutableRoaringBitmap.and(audience.impl(), bs.impl()).getLongCardinality).getOrElse(0L)
        }.toList
      }
    }
  }

  private def getAudience(bitsets: Map[Bucket, Map[Dimension, Map[Value, Bitset[ImmutableRoaringBitmap]]]], filter: Filter)(implicit metaDao: MetaDao): Bitset[ImmutableRoaringBitmap] = {
    val ored: List[MutableRoaringBitmap] = filter.f.map{ and: List[Map[Bucket, Element]] =>
      val ors: Seq[ImmutableRoaringBitmap] = and.flatMap{ or: Map[Bucket, Element] =>
        or.flatMap{case(bucket, element) =>
          element.e.flatMap{
//            case(dimension, Value("*") :: Nil) =>
//              val values = metaDao.getDimensionValues(dimension :: Nil).flatMap(_._2)
//              values.map{ value =>
//                Try(bitsets(bucket)(dimension)(value)).toOption
//              }
            case(dimension, values) =>
              values.map{ value =>
                Try(bitsets(bucket)(dimension)(value)).toOption
              }
          }
        }
        .flatten
        .map(_.impl())
        .toSeq
      }
      ImmutableRoaringBitmap.or(ors: _*)
    }
    new ImmutableRoaringBitmapWrapper(ImmutableRoaringBitmap.and(ored.toIterator.asJava, 0L, 1L << 32))
  }

}