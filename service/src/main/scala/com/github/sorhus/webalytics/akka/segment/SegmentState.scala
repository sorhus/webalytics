package com.github.sorhus.webalytics.akka.segment

import java.io.{DataOutputStream, File, FileOutputStream, RandomAccessFile}
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel.MapMode

import com.github.sorhus.webalytics.akka.event._
import com.github.sorhus.webalytics.model._
import org.roaringbitmap.{ImmutableBitmapDataProvider, RoaringBitmap}
import org.roaringbitmap.buffer.ImmutableRoaringBitmap
import org.slf4j.LoggerFactory

import scala.collection.immutable.Iterable
import scala.collection.mutable.{Map => MMap, Set => MSet}

abstract class ASegmentState[T](bitsetOps: BitsetOps[T]) extends Serializable {

  @transient val log = LoggerFactory.getLogger(getClass)

  def bitsets: MapWrapper[T]

  def getAllBuckets: Iterable[Bucket]

  def getCount(query: Query, space: Map[Dimension, Set[Value]]): Iterable[(Bucket, Iterable[(Dimension, Iterable[(Value, Long)])])] = {
    val audience: Bitset[T] = getAudience(query.filter)
    query.buckets.map{ bucket =>
      bucket -> space.map{case(dimension, values) =>
        dimension -> values.flatMap{value =>
          val bitset: Option[Bitset[T]] = bitsets.getOption(bucket, dimension, value)
          bitset.map { bs =>
            value -> bitsetOps.andCardinality(audience.impl(), bs.impl())
          }
        }
      }
    }
  }

  def getCounts(domain: Element): Iterable[(Bucket, Iterable[(Dimension, Iterable[(Value, Long)])])] = {
    getAllBuckets.map{bucket =>
      bucket -> domain.e.map{case(dimension, values) =>
        dimension -> values.flatMap{value =>
          val bitset: Option[Bitset[T]] = bitsets.getOption(bucket, dimension, value)
          bitset.map{bs =>
            value -> bs.cardinality()
          }
        }.toList
      }.toList
    }.toList
  }

  private def getAudience(filter: Filter): Bitset[T] = {
    val toAnd: List[Bitset[T]] = filter.f.map{ and: List[Map[Bucket, Element]] =>
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
      bitsetOps.or(toOr.map(_.impl()))
    }
    bitsetOps.and(toAnd.map(_.impl()))
  }

}

class MutableSegmentState extends ASegmentState[RoaringBitmap](MutableBitsetOps) {

  val bitsets = new MutableMapWrapper

  def getCopy(bucket: Bucket): Map[Dimension, Map[Value, Bitset[RoaringBitmap]]] =
    bitsets.get(bucket).map{case(d,values) =>
      d -> values.map{case(v,b) =>
        v -> b.getCopy
      }.toMap
    }.toMap

  def get(buckets: List[Bucket]): MMap[Dimension, MMap[Value, MutableBitset[RoaringBitmap]]] = {
    buckets.flatMap(bitsets.bitsets.get).reduce(_ ++ _)
  }

  def post(event: PostEvent): Unit = {
    event.element.e.foreach{case (dimension, values) =>
      values.foreach{ value =>
        bitsets.get(event.bucket, dimension, value)
          .set(event.documentId.d, value = true)
      }
    }
  }

  def remove(bucket: Bucket) = {
    bitsets.remove(bucket)
  }

  override def getAllBuckets: Iterable[Bucket] = bitsets.bitsets.keys.toList
}

class ImmutableSegmentState(path: String) extends ASegmentState[ImmutableBitmapDataProvider](ImmutableBitsetOps) {

  val bitsets = new ImmutableMapWrapper

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

  def read(bucket: Bucket, space: Element) = {
    val files = MSet[RandomAccessFile]()
    val dir = s"$path/${bucket.b}"
    log.info("Asked and received space {}", space)
    val result = space.e.map{ case(dimension, values) =>
      val name = s"$dir/${dimension.d}"
      log.info("Listing {}", name)
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

    // This should be somewhere else (on recieve shutdown)
    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
      override def run(): Unit = {
        files.foreach(_.close())
      }
    }))

    bitsets.put(bucket, result)
  }

  override def getAllBuckets: Iterable[Bucket] = bitsets.bitsets.keys.toList
}

case class QuerySegmentState(mutableState: MutableSegmentState, immutableState: Option[ImmutableSegmentState] = None)
  extends ASegmentState[ImmutableBitmapDataProvider](ImmutableBitsetOps) {

  def update(state: ImmutableSegmentState) = copy(immutableState = Some(state))

  def bitsets = new QueryMapWrapper(mutableState.bitsets, immutableState.get.bitsets)

  override def getAllBuckets: Iterable[Bucket] = mutableState.getAllBuckets ++ immutableState.map(_.getAllBuckets).getOrElse(Nil)
}