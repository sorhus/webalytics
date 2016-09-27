package com.github.sorhus.webalytics.model

trait Bitset[T] {
  def set(bit: Long, value: Boolean): Unit
  def and(that: Bitset[T]): Unit
  def and(bitset1: Bitset[T], bitset2: Bitset[T]): Bitset[T]
  def or(that: Bitset[T]): Unit
  def cardinality(): Long
  def create(): Bitset[T]
  def impl(): T
}
