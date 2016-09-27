package com.github.sorhus.webalytics.impl

import java.nio.ByteBuffer

import com.github.sorhus.webalytics.model.Bitset
import com.zaxxer.sparsebits.SparseBitSet
import org.roaringbitmap.RoaringBitmap
import org.roaringbitmap.buffer.ImmutableRoaringBitmap

class SparseBitSetWrapper(val impl: SparseBitSet = new SparseBitSet()) extends Bitset[SparseBitSet] {
  override def set(bit: Long, value: Boolean): Unit = impl.set(bit.toInt, value)
  override def and(that: Bitset[SparseBitSet]): Unit = {
    impl.and(that.impl())
  }
  def and(bitSet1: Bitset[SparseBitSet], bitset2: Bitset[SparseBitSet]): Bitset[SparseBitSet] = {
    new SparseBitSetWrapper(SparseBitSet.and(bitSet1.impl(), bitset2.impl()))
  }
  override def or(that: Bitset[SparseBitSet]): Unit = impl.or(that.impl())
  override def cardinality(): Long = impl.cardinality()
  override def create(): Bitset[SparseBitSet] = new SparseBitSetWrapper(new SparseBitSet())
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

class ImmutableRoaringMitmapWrapper(val impl: ImmutableRoaringBitmap = new ImmutableRoaringBitmap(ByteBuffer.allocate(1))) extends Bitset[ImmutableRoaringBitmap] {
  override def set(bit: Long, value: Boolean): Unit = ???
  override def and(that: Bitset[ImmutableRoaringBitmap]): Unit = {
    new ImmutableRoaringMitmapWrapper(ImmutableRoaringBitmap.and(impl, that.impl()))
  }
  override def and(bitset1: Bitset[ImmutableRoaringBitmap], bitset2: Bitset[ImmutableRoaringBitmap]): Bitset[ImmutableRoaringBitmap] = {
    new ImmutableRoaringMitmapWrapper(ImmutableRoaringBitmap.and(bitset1.impl(), bitset2.impl()))
  }
  override def or(that: Bitset[ImmutableRoaringBitmap]): Unit = {
    new ImmutableRoaringMitmapWrapper(ImmutableRoaringBitmap.or(impl, that.impl()))
  }
  override def cardinality(): Long = impl.getLongCardinality
  override def create(): Bitset[ImmutableRoaringBitmap] = ???
}