package com.github.sorhus.webalytics.akka.segment

import java.io.{DataOutputStream, File, FileOutputStream, RandomAccessFile}
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel.MapMode
import java.util.concurrent.TimeUnit

import akka.util.Timeout
import com.github.sorhus.webalytics.akka.model._
import org.roaringbitmap.RoaringBitmap
import org.roaringbitmap.buffer.ImmutableRoaringBitmap
import org.slf4j.LoggerFactory

import scala.collection.mutable.{Set => MSet}

abstract class ASegmentState[T](bitsetWrapper: BitsetWrapper[T]) extends Serializable {

  val log = LoggerFactory.getLogger(getClass)

  implicit val bitsets: MapWrapper[T]

  def getCount(query: Query, space: Map[Dimension, Set[Value]]): List[(Bucket, List[(Dimension, List[(Value, Long)])])] = {
    val audience: Bitset[T] = getAudience(query.filter)
    query.buckets.map{ bucket =>
      bucket -> space.map{case(dimension, values) =>
        dimension -> values.map{value =>
          val bitset: Option[Bitset[T]] = bitsets.getOption(bucket, dimension, value)
          value -> bitset.map(bs => bitsetWrapper.andCard(audience.impl(), bs.impl())).getOrElse(0L)
        }.toList
      }.toList
    }
  }


  private def getAudience(filter: Filter): Bitset[T] = {
    val toAnd: List[T] = filter.f.map{ and: List[Map[Bucket, Element]] =>
      val toOr: List[Bitset[T]] = and.flatMap{ or: Map[Bucket, Element] =>
        or.flatMap{case(bucket, element) =>
          element.e.flatMap{
            case(dimension, values) =>
              values.flatMap { value =>
                bitsets.getOption(bucket, dimension, value)
              }
          }
        }
      }
      bitsetWrapper.or(toOr.map(_.impl()))
    }
    bitsetWrapper.cons(bitsetWrapper.and(toAnd))
  }

}

class MutableSegmentState extends ASegmentState[RoaringBitmap](MutableBitmap) {

  val bitsets = MutableMapWrapper

  def post(event: PostEvent): Unit = {
    event.element.e.foreach{case (dimension, values) =>
      values.foreach{ value =>
        bitsets.get(event.bucket, dimension, value).set(event.documentId.d, value = true)
      }
    }
  }

  def remove(bucket: Bucket) = {
    bitsets.remove(bucket)
  }

}

class ImmutableSegmentState(path: String) extends ASegmentState[ImmutableRoaringBitmap](ImmutableBitMap) {

  implicit val timeout = Timeout(1, TimeUnit.MINUTES)

  val bitsets = ImmutableMapWrapper

  // TODO This should be a plugin
  def write(bucket: Bucket, bitsets: Map[Dimension, Map[Value, Bitset[RoaringBitmap]]]) = {
    bitsets.foreach{case(dimension, values) =>
      val file = new File(s"$path/${bucket.b}/${dimension.d}")
      file.getParentFile.mkdirs()
      file.createNewFile()
      val fos = new FileOutputStream(file)
      val dos = new DataOutputStream(fos)
      val bytes = 0
      values.toList.sortBy(_._1.v).foreach{case(value, bitset) =>
        bitset.impl().runOptimize()
        log.info("Writing bitset: {} with bytes: ", (bucket.b, dimension.d, value.v, bitset.cardinality(), bitset.impl().serializedSizeInBytes()))
        bitset.impl().serialize(dos)
      }
      log.info("Closing outputstream for {}", dimension)
      dos.close()
    }
  }

  def read(bucket: Bucket, space: Element)(implicit timout: Timeout) = {
    val files = MSet[RandomAccessFile]()
    val dir = s"$path/${bucket.b}"
    log.info("Listing dimensions in {}", dir)
    val dimensions = new File(dir).list().toList.map(Dimension.apply)
    log.info("Listing bucket dir, found dimensions: {}", dimensions)
    log.info("Asked and received space {}", space)
    val result = Map {
      bucket -> space.e.map{ case(dimension, values) =>
        val name = s"$dir/${dimension.d}"
        val file = new RandomAccessFile(name, "r")
        files.add(file)
        log.info("Memory mapping file {}", s"$name: ${file.length()}")
        val memoryMapped: MappedByteBuffer = file.getChannel.map(MapMode.READ_ONLY, 0, file.length())
        val bb = memoryMapped.slice()
        log.info(s"got bytebuffer {}", bb)
        dimension -> values.toList.sortBy(_.v).map{value =>
          val bitset = new ImmutableRoaringBitmap(bb)
          log.info("Read bitset: {}", (bucket, dimension.d, value.v, bitset.getCardinality))
          log.info("At position: {}", (bb.position(), bitset.serializedSizeInBytes()))
          bb.position(bb.position() + bitset.serializedSizeInBytes())
          value -> new ImmutableRoaringBitmapWrapper(bitset)
        }.toMap
      }
    }

    // This should be somewhere else (on recieve shutdown)
    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
      override def run(): Unit = {
        files.foreach(_.close())
      }
    }))

    bitsets.set(result)
  }
}