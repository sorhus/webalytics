package com.github.sorhus.webalytics

import java.io.{DataOutputStream, File, FileOutputStream, RandomAccessFile}
import java.nio.channels.FileChannel.MapMode

import org.roaringbitmap.RoaringBitmap
import org.roaringbitmap.buffer.ImmutableRoaringBitmap

object ImmutableTest extends App {

  val outFile = new File(args(0))
  outFile.createNewFile()
  val fos = new FileOutputStream(outFile)
  val dos = new DataOutputStream(fos)
  val x = RoaringBitmap.bitmapOf(1,3,5)
  x.runOptimize()
  x.serialize(dos)
  dos.close()

  val inFile = new RandomAccessFile(args(0), "r")
  val memoryMapped = inFile.getChannel.map(MapMode.READ_ONLY, 0, inFile.length())
  val bb = memoryMapped.slice()
  val bitset = new ImmutableRoaringBitmap(bb)

  println(ImmutableRoaringBitmap.and(bitset,bitset).getCardinality)
  println(ImmutableRoaringBitmap.andCardinality(bitset,bitset))

  inFile.close()
}
