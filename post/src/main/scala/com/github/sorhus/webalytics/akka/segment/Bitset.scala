package com.github.sorhus.webalytics.akka.segment

import com.github.sorhus.webalytics.akka.model.{Bucket, Dimension, Element, Value}
import org.roaringbitmap.{FastAggregation, RoaringBitmap}
import org.roaringbitmap.buffer.{BufferFastAggregation, ImmutableRoaringBitmap}

import scala.collection.mutable.{Map => MMap}
import scala.util.Try

trait Bitset[T] extends Serializable {
  def set(bit: Long, value: Boolean): Unit
  def and(that: Bitset[T]): Unit
  def and(bitset1: Bitset[T], bitset2: Bitset[T]): Bitset[T]
  def or(that: Bitset[T]): Unit
  def cardinality(): Long
  def create(): Bitset[T]
  def impl(): T
}


class RoaringBitmapWrapper(val impl: RoaringBitmap = new RoaringBitmap()) extends Bitset[RoaringBitmap] {
  override def set(bit: Long, value: Boolean): Unit = if(value) impl.add(bit.toInt) else impl.remove(bit.toInt)
  override def and(that: Bitset[RoaringBitmap]): Unit = impl.and(that.impl())
  override def and(bitset1: Bitset[RoaringBitmap], bitset2: Bitset[RoaringBitmap]): Bitset[RoaringBitmap] = {
    new RoaringBitmapWrapper(RoaringBitmap.and(bitset1.impl(), bitset2.impl()))
  }
  override def or(that: Bitset[RoaringBitmap]): Unit = impl.or(that.impl())
  override def cardinality(): Long = impl.getLongCardinality
  override def create(): Bitset[RoaringBitmap] = new RoaringBitmapWrapper(new RoaringBitmap())
}

class ImmutableRoaringBitmapWrapper(val impl: ImmutableRoaringBitmap) extends Bitset[ImmutableRoaringBitmap] {
  override def set(bit: Long, value: Boolean): Unit = ???
  override def and(that: Bitset[ImmutableRoaringBitmap]): Unit = {
    new ImmutableRoaringBitmapWrapper(ImmutableRoaringBitmap.and(impl, that.impl()))
  }
  override def and(bitset1: Bitset[ImmutableRoaringBitmap], bitset2: Bitset[ImmutableRoaringBitmap]): Bitset[ImmutableRoaringBitmap] = {
    new ImmutableRoaringBitmapWrapper(ImmutableRoaringBitmap.and(bitset1.impl(), bitset2.impl()))
  }
  override def or(that: Bitset[ImmutableRoaringBitmap]): Unit = {
    new ImmutableRoaringBitmapWrapper(ImmutableRoaringBitmap.or(impl, that.impl()))
  }
  override def cardinality(): Long = impl.getLongCardinality
  override def create(): Bitset[ImmutableRoaringBitmap] = ???
}
object ImmutableRoaringBitmapWrapper {
  def and(bitset1: Bitset[ImmutableRoaringBitmap], bitset2: Bitset[ImmutableRoaringBitmap]): Bitset[ImmutableRoaringBitmap] = {
    new ImmutableRoaringBitmapWrapper(ImmutableRoaringBitmap.and(bitset1.impl(), bitset2.impl()))
  }
}

trait MapWrapper[T] extends {

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
  var bitsets = MMap[Bucket, MMap[Dimension, MMap[Value, Bitset[RoaringBitmap]]]]()

  override def getOption(bucket: Bucket, dimension: Dimension, value: Value): Option[Bitset[RoaringBitmap]] = Try {
    bitsets(bucket)(dimension)(value)
  }.toOption

  def get(bucket: Bucket, dimension: Dimension, value: Value): Bitset[RoaringBitmap] = Try {
    bitsets(bucket)(dimension)(value)
  }.getOrElse{
    makeBitsetsExist(bucket, dimension, value)
  }

  def remove(bucket: Bucket) = bitsets.remove(bucket)

  private def makeBitsetsExist(bucket: Bucket, dimension: Dimension, value: Value): Bitset[RoaringBitmap] = {
    if(!bitsets.contains(bucket)) {
      bitsets.put(bucket, MMap[Dimension, MMap[Value, Bitset[RoaringBitmap]]]())
    }
    if(!bitsets(bucket).contains(dimension)) {
      bitsets(bucket).put(dimension, MMap[Value, Bitset[RoaringBitmap]]())
    }
    if(!bitsets(bucket)(dimension).contains(value)) {
      bitsets(bucket)(dimension).put(value, MutableBitmap.cons())
    }
    bitsets(bucket)(dimension)(value)
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
          bitsets(bucket)(dimension).put(value, MutableBitmap.cons())
        }
      }
    }
  }

  def get(bucket: Bucket): MMap[Dimension, MMap[Value, Bitset[RoaringBitmap]]] = bitsets(bucket)
}

trait BitsetWrapper[T] {
  def cons(impl: T): Bitset[T]
  def or(toOr: Iterable[T]): T
  def and(toAnd: Iterable[T]): T
  def andCard(x: T, y: T): Long
}

object MutableBitmap extends BitsetWrapper[RoaringBitmap] {
  def cons(): Bitset[RoaringBitmap] = new RoaringBitmapWrapper()
  override def cons(impl: RoaringBitmap): Bitset[RoaringBitmap] = new RoaringBitmapWrapper(impl)
  override def or(toOr: Iterable[RoaringBitmap]): RoaringBitmap = FastAggregation.or(toOr.toSeq: _*)
  override def and(toAnd: Iterable[RoaringBitmap]): RoaringBitmap = FastAggregation.and(toAnd.toSeq: _*)
  override def andCard(x: RoaringBitmap, y: RoaringBitmap): Long = RoaringBitmap.andCardinality(x,y)
}

object ImmutableBitMap extends BitsetWrapper[ImmutableRoaringBitmap] {
  override def cons(impl: ImmutableRoaringBitmap): Bitset[ImmutableRoaringBitmap] = new ImmutableRoaringBitmapWrapper(impl)
  override def or(toOr: Iterable[ImmutableRoaringBitmap]): ImmutableRoaringBitmap = BufferFastAggregation.or(toOr.toSeq: _*)
  override def and(toAnd: Iterable[ImmutableRoaringBitmap]): ImmutableRoaringBitmap = BufferFastAggregation.and(toAnd.toSeq: _*)
  override def andCard(x: ImmutableRoaringBitmap, y: ImmutableRoaringBitmap): Long = ImmutableRoaringBitmap.andCardinality(x,y)
}