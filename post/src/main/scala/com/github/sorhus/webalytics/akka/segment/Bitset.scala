package com.github.sorhus.webalytics.akka.segment

import com.github.sorhus.webalytics.akka.model.{Bucket, Dimension, Value}
import org.roaringbitmap.{FastAggregation, RoaringBitmap}
import org.roaringbitmap.buffer.{BufferFastAggregation, ImmutableRoaringBitmap}

import scala.collection.mutable.{Map => MMap}
import scala.util.Try

trait Bitset[T] extends Serializable {
  def cardinality(): Long
  def impl(): T
}

trait MutableBitset[T] extends Bitset[T] {
  def getCopy: Bitset[T]
  def set(bit: Long, value: Boolean): Unit

}

class RoaringBitmapWrapper(val impl: RoaringBitmap = new RoaringBitmap()) extends MutableBitset[RoaringBitmap] {
  override def set(bit: Long, value: Boolean): Unit = if(value) impl.add(bit.toInt) else impl.remove(bit.toInt)
  override def cardinality(): Long = impl.getLongCardinality
  override def getCopy: Bitset[RoaringBitmap] = new RoaringBitmapWrapper(impl.clone())
}

class ImmutableRoaringBitmapWrapper(val impl: ImmutableRoaringBitmap) extends Bitset[ImmutableRoaringBitmap] {
  override def cardinality(): Long = impl.getLongCardinality
}

trait BitsetOps[T] extends Serializable {
  def or(toOr: Iterable[T]): Bitset[T]
  def and(toAnd: Iterable[T]): Bitset[T]
  def andCardinality(x: T, y: T): Long
}

object MutableBitsetOps extends BitsetOps[RoaringBitmap] {
  def cons(): MutableBitset[RoaringBitmap] = new RoaringBitmapWrapper()
  private def cons(impl: RoaringBitmap): Bitset[RoaringBitmap] = new RoaringBitmapWrapper(impl)
  override def or(toOr: Iterable[RoaringBitmap]): Bitset[RoaringBitmap] = cons(FastAggregation.or(toOr.toSeq: _*))
  override def and(toAnd: Iterable[RoaringBitmap]): Bitset[RoaringBitmap] = cons(FastAggregation.and(toAnd.toSeq: _*))
  override def andCardinality(x: RoaringBitmap, y: RoaringBitmap): Long = RoaringBitmap.andCardinality(x,y)
}

object ImmutableBitsetOps extends BitsetOps[ImmutableRoaringBitmap] {
  private def cons(impl: ImmutableRoaringBitmap): Bitset[ImmutableRoaringBitmap] = new ImmutableRoaringBitmapWrapper(impl)
  override def or(toOr: Iterable[ImmutableRoaringBitmap]): Bitset[ImmutableRoaringBitmap] = cons(BufferFastAggregation.or(toOr.toSeq: _*))
  override def and(toAnd: Iterable[ImmutableRoaringBitmap]): Bitset[ImmutableRoaringBitmap] = cons(BufferFastAggregation.and(toAnd.toSeq: _*))
  override def andCardinality(x: ImmutableRoaringBitmap, y: ImmutableRoaringBitmap): Long = ImmutableRoaringBitmap.and(x,y).getLongCardinality
}


trait MapWrapper[T] extends Serializable {

  def getOption(bucket: Bucket, dimension: Dimension, value: Value): Option[Bitset[T]]
}

object ImmutableMapWrapper extends MapWrapper[ImmutableRoaringBitmap] {
  var bitsets = Map[Bucket, Map[Dimension, Map[Value, Bitset[ImmutableRoaringBitmap]]]]()

  def set(bs: Map[Bucket, Map[Dimension, Map[Value, ImmutableRoaringBitmapWrapper]]]) = {
    bitsets = bs
  }
  override def getOption(bucket: Bucket, dimension: Dimension, value: Value): Option[Bitset[ImmutableRoaringBitmap]] = Try {
    bitsets(bucket)(dimension)(value)
  }.toOption
}

object MutableMapWrapper extends MapWrapper[RoaringBitmap] {
  var bitsets = MMap[Bucket, MMap[Dimension, MMap[Value, MutableBitset[RoaringBitmap]]]]()

  override def getOption(bucket: Bucket, dimension: Dimension, value: Value): Option[Bitset[RoaringBitmap]] = Try {
    bitsets(bucket)(dimension)(value)
  }.toOption

  def get(bucket: Bucket, dimension: Dimension, value: Value): MutableBitset[RoaringBitmap] = Try {
    bitsets(bucket)(dimension)(value)
  }.getOrElse{
    makeBitsetsExist(bucket, dimension, value)
  }

  def remove(bucket: Bucket) = bitsets.remove(bucket)

  private def makeBitsetsExist(bucket: Bucket, dimension: Dimension, value: Value): MutableBitset[RoaringBitmap] = {
    if(!bitsets.contains(bucket)) {
      bitsets.put(bucket, MMap[Dimension, MMap[Value, MutableBitset[RoaringBitmap]]]())
    }
    if(!bitsets(bucket).contains(dimension)) {
      bitsets(bucket).put(dimension, MMap[Value, MutableBitset[RoaringBitmap]]())
    }
    if(!bitsets(bucket)(dimension).contains(value)) {
      bitsets(bucket)(dimension).put(value, MutableBitsetOps.cons())
    }
    bitsets(bucket)(dimension)(value)
  }

  def get(bucket: Bucket): MMap[Dimension, MMap[Value, MutableBitset[RoaringBitmap]]] = bitsets(bucket)
}

