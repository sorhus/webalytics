package com.github.sorhus.webalytics.akka.segment

import java.io.{DataOutputStream, File, FileOutputStream, RandomAccessFile}
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel.MapMode
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, Props}
import akka.util.Timeout
import com.github.sorhus.webalytics.impl.ImmutableRoaringBitmapWrapper
import com.github.sorhus.webalytics.model._
import org.roaringbitmap.RoaringBitmap
import org.roaringbitmap.buffer.{BufferFastAggregation, ImmutableRoaringBitmap}
import org.slf4j.LoggerFactory

import scala.collection.mutable.{Map => MMap, Set => MSet}
import scala.util.Try

class ImmutableBitsetActor(path: String) extends Actor {

  val log = LoggerFactory.getLogger(getClass)

  implicit val timeout = Timeout(1, TimeUnit.MINUTES)

  var state: Map[Bucket, Map[Dimension, Map[Value, Bitset[ImmutableRoaringBitmap]]]] = Map.empty

  override def receive: Receive = {

    case QueryEvent(query: Query, space: Element) =>
      log.info("received query and space {}", (query, space))
      val response: Map[String, Map[String, Map[String, Long]]] = getCount(query, space.e)
        .map{case(bucket, dimensions) =>
          bucket.b -> dimensions.map{case(dimension, values) =>
            dimension.d -> values.map{case(value, count) =>
              value.v -> count
            }.toMap
          }.toMap
        }.toMap
      sender() ! response

    case LoadImmutable(bucket, space) =>
      state = read(bucket, space.get)
      sender() ! Ack

    case MakeImmutable(bucket, bitsets) =>
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
      sender() ! Ack
  }

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

  def read(bucket: Bucket, space: Element)(implicit timout: Timeout): Map[Bucket, Map[Dimension, Map[Value, Bitset[ImmutableRoaringBitmap]]]] = {
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

    result
  }

  def getCount(query: Query, dimensionValues: Map[Dimension, Set[Value]]): List[(Bucket, List[(Dimension, List[(Value, Long)])])] = {
    val audience = getAudience(query.filter)
    query.buckets.map{ bucket =>
      bucket -> dimensionValues.map{case(dimension, values) =>
        dimension -> values.map{value =>
          val bitset: Option[Bitset[ImmutableRoaringBitmap]] = Try(state(bucket)(dimension)(value)).toOption
          log.info("Found roaring for {}", (dimension, value, bitset))
          value -> bitset.map(bs => ImmutableRoaringBitmapWrapper.and(audience, bs).cardinality()).getOrElse(0L)
        }.toList
      }.toList
    }
  }

  private def getAudience(filter: Filter): Bitset[ImmutableRoaringBitmap] = {
    log.info("Computing audience for {}", filter)
    val toAnd: List[ImmutableRoaringBitmap] = filter.f.map{ and: List[Map[Bucket, Element]] =>
      val toOr: List[Bitset[ImmutableRoaringBitmap]] = and.flatMap{ or: Map[Bucket, Element] =>
        or.toList.flatMap{case(bucket, element) =>
          element.e.flatMap{
            case(dimension, values) =>
              values.flatMap { value =>
                val bs = Try(state(bucket)(dimension)(value)).toOption
                log.info("Adding bitset to ored: {}", (dimension, value, bs))
                bs
              }
          }
        }
      }
      BufferFastAggregation.or(toOr.map(_.impl()): _*).toImmutableRoaringBitmap
    }
    val result: ImmutableRoaringBitmap = BufferFastAggregation.and(toAnd: _*).toImmutableRoaringBitmap
    log.info("Audience computed with cardinality {}", result.getCardinality)
    new ImmutableRoaringBitmapWrapper(result)
  }

}

object ImmutableBitsetActor {
  def props(path: String) = Props(new ImmutableBitsetActor(path))
}

case class MakeImmutable(bucket: Bucket, bitsets: MMap[Dimension, MMap[Value, Bitset[RoaringBitmap]]] = MMap())