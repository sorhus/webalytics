package com.github.sorhus.webalytics.akka.segment

import com.github.sorhus.webalytics.akka.model.{Bucket, Dimension, Value}
import org.roaringbitmap.{FastAggregation, ImmutableBitmapDataProvider, RoaringBitmap}
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

class ImmutableRoaringBitmapWrapper(val impl: ImmutableBitmapDataProvider) extends Bitset[ImmutableBitmapDataProvider] {
  override def cardinality(): Long = impl.getLongCardinality
//  def toImmutable: Bitset[ImmutableRoaringBitmap] = {
//    impl.
//  }

}

trait BitsetOps[T] extends Serializable {
  def or(toOr: Iterable[T]): Bitset[T]
  def and(toAnd: Iterable[T]): Bitset[T]
  def andCardinality(x: T, y: T): Long
}

object MutableBitsetOps extends BitsetOps[RoaringBitmap] {
  def cons(): RoaringBitmapWrapper = new RoaringBitmapWrapper()
  private def cons(impl: RoaringBitmap): RoaringBitmapWrapper = new RoaringBitmapWrapper(impl)
  override def or(toOr: Iterable[RoaringBitmap]): RoaringBitmapWrapper = cons(FastAggregation.or(toOr.toSeq: _*))
  override def and(toAnd: Iterable[RoaringBitmap]): RoaringBitmapWrapper = cons(FastAggregation.and(toAnd.toSeq: _*))
  override def andCardinality(x: RoaringBitmap, y: RoaringBitmap): Long = RoaringBitmap.andCardinality(x,y)
}

object ImmutableBitsetOps extends BitsetOps[ImmutableBitmapDataProvider] {
  private def cons(impl: ImmutableBitmapDataProvider) = new ImmutableRoaringBitmapWrapper(impl)
  override def or(toOr: Iterable[ImmutableBitmapDataProvider]) = cons(BufferFastAggregation.or(toOr.map(_.asInstanceOf[ImmutableRoaringBitmap]).toSeq: _*))
  override def and(toAnd: Iterable[ImmutableBitmapDataProvider]) = cons(BufferFastAggregation.and(toAnd.map(_.asInstanceOf[ImmutableRoaringBitmap]).toSeq: _*))
  override def andCardinality(x: ImmutableBitmapDataProvider, y: ImmutableBitmapDataProvider): Long = ImmutableRoaringBitmap.and(x.asInstanceOf[ImmutableRoaringBitmap],y.asInstanceOf[ImmutableRoaringBitmap]).getLongCardinality
}


trait MapWrapper[T] extends Serializable {

//  def getOption(bucket: Bucket, dimension: Dimension, value: Value): Option[Bitset[T]]
  def getOption(bucket: Bucket, dimension: Dimension, value: Value): Option[Bitset[T]]
}

// TODO make class
class ImmutableMapWrapper extends MapWrapper[ImmutableBitmapDataProvider] {
  var bitsets = Map[Bucket, Map[Dimension, Map[Value, Bitset[ImmutableBitmapDataProvider]]]]()

  def put(bucket: Bucket, bs: Map[Dimension, Map[Value, ImmutableRoaringBitmapWrapper]]) = {
    bitsets = bitsets + (bucket -> bs)
  }

  override def getOption(bucket: Bucket, dimension: Dimension, value: Value): Option[Bitset[ImmutableBitmapDataProvider]] = Try {
    bitsets(bucket)(dimension)(value)
  }.toOption
}

// TODO make class
class MutableMapWrapper extends MapWrapper[RoaringBitmap] {

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

class QueryMapWrapper(mutable: MutableMapWrapper, immutable: ImmutableMapWrapper) extends MapWrapper[ImmutableBitmapDataProvider] {
  override def getOption(bucket: Bucket, dimension: Dimension, value: Value): Option[Bitset[ImmutableBitmapDataProvider]] = {
    val y = mutable.getOption(bucket, dimension, value).map { m =>
      val i = m.impl()
      val n = i.toMutableRoaringBitmap.toImmutableRoaringBitmap
      new ImmutableRoaringBitmapWrapper(n)
    }

//      .orElse(
    val x = immutable.getOption(bucket, dimension, value).map(_.impl()).map(m => new ImmutableRoaringBitmapWrapper(m))

    val z: Option[ImmutableRoaringBitmapWrapper] = y.orElse(x)

    z
  }
}