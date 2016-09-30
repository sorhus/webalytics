package com.github.sorhus.webalytics.batch

import java.io._
import java.nio.channels.FileChannel.MapMode
import java.util.UUID

import akka.actor.ActorSystem
import com.github.sorhus.webalytics.impl.{ImmutableRoaringMitmapWrapper, RoaringBitmapWrapper}
import com.github.sorhus.webalytics.impl.redis.RedisMetaDao
import com.github.sorhus.webalytics.model._
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.json4s.{DefaultFormats, Formats}
import org.json4s.jackson.JsonMethods._
import org.roaringbitmap.RoaringBitmap
import org.roaringbitmap.buffer.ImmutableRoaringBitmap
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.util.Try

/**
  * Batch load data into a bitset.
  *
  *
  * Use cases:
  *   1. We need to be able to update elements and add new elements
  *   2. We don't need to update elements, but we need to be able to add elements
  *   3. We don't need to update nor add elements
  *
  * Element ids can be attached or not attached.
  *
  * We always need to store meta data regarding dimensions and values
  * Only in case of 1) do we need to store (element id -> document id) mapping
  * In case of 3) we need no additional meta data
  */
object BitsetLoader extends App {

  val inputFile = args(0)
  val bucket = Bucket(args(1))
  val outputDir = args(2)

  val in = new BufferedReader(new FileReader(inputFile))
  val out = new BufferedWriter(new OutputStreamWriter(System.out))
  val audienceDao = new BitsetDao[RoaringBitmap](new RoaringBitmapWrapper().create _)

  implicit val system = {
    val config = ConfigFactory.load()
      .withValue("akka.loglevel", ConfigValueFactory.fromAnyRef("OFF"))
      .withValue("akka.stdout-loglevel", ConfigValueFactory.fromAnyRef("OFF"))
    ActorSystem("webalytics-bitset-loader", config)
  }

  import scala.concurrent.ExecutionContext.Implicits.global

  val metaDao = Try(args(3)).toOption match {
    case Some("devnull") => new DevNullMetaDao
    case _ => new DelayedBatchInsertMetaDao(new RedisMetaDao())
  }
  val loader = new BitsetLoader()
  loader.load(bucket, in, audienceDao)(metaDao)
  Try(metaDao.asInstanceOf[DelayedBatchInsertMetaDao]).map(_.commit())
  system.shutdown()
  loader.write(outputDir, audienceDao)

}
class BitsetLoader {

  val log = LoggerFactory.getLogger(getClass)

  implicit val jsonFormats: Formats = DefaultFormats

  def load(bucket: Bucket, in: BufferedReader, audienceDao: BitsetDao[RoaringBitmap])(implicit metaDao: MetaDao): Unit = {
    Stream.continually(in.readLine())
      .takeWhile(_ != null)
      .map(_.split("\t"))
      .map {
        case Array(elementId, json) =>
          val element = parse(json)
            .extract[Map[String, List[String]]]
            .map { case (dimension: String, values: List[String]) =>
              Dimension(dimension) -> values.map(v => Value(v))
            }
          (ElementId(elementId), Element(element))
        case Array(json) =>
          val element = parse(json)
            .extract[Map[String, List[String]]]
            .map { case (dimension: String, values: List[String]) =>
              Dimension(dimension) -> values.map(v => Value(v))
            }
          (ElementId(UUID.randomUUID().toString), Element(element))
      }
      .foreach { case (elementId, element) =>
        audienceDao.post(bucket, elementId, element)
      }
  }

  def write(path: String, audienceDao: BitsetDao[RoaringBitmap]) = {
    audienceDao.bitsets.foreach{case(bucket, dimvals) =>
      dimvals.foreach{case(dimension, values) =>
        val file = new File(s"$path/${bucket.b}/${dimension.d}")
        file.getParentFile.mkdirs()
        file.createNewFile()
        val fos = new FileOutputStream(file)
        val dos = new DataOutputStream(fos)
        values.toList.sortBy(_._1.v).foreach{case(value, bitset) =>
//          log.info("Writing bitset: {}\n", (bucket.b, dimension.d, value.v, bitset.cardinality()))
          bitset.impl().runOptimize()
          bitset.impl().serialize(dos)
        }
        dos.close()
      }
    }
  }

  def read(path: String)(implicit metaDao: MetaDao): Map[Bucket, Map[Dimension, Map[Value, Bitset[ImmutableRoaringBitmap]]]] = {
    val files = mutable.Set[RandomAccessFile]()
    val result = new File(path).list().map { bucket =>
      val dimensions = new File(s"$path/$bucket").list().toList.map(Dimension.apply)
      val dimVals = metaDao.getDimensionValues(dimensions)
      if(dimVals.isEmpty) {
        throw new RuntimeException("found no values for dimensions ${dimensions.map(_.d)}")
      }
      Bucket(bucket) -> dimVals.map{ case(dimension, values) =>
        val file = new RandomAccessFile(s"$path/$bucket/${dimension.d}", "r")
        files.add(file)
        val memoryMapped = file.getChannel.map(MapMode.READ_ONLY, 0, file.length())
        val bb = memoryMapped.slice()
        dimension -> values.sortBy(_.v).map{value =>
          val bitset = new ImmutableRoaringBitmap(bb)
          log.info("Read bitset: {}\n", (bucket, dimension.d, value.v, bitset.getCardinality))
          bb.position(bb.position() + bitset.serializedSizeInBytes())
          value -> new ImmutableRoaringMitmapWrapper(bitset)
        }.toMap
      }.toMap
    }.toMap

    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
      override def run(): Unit = {
        files.foreach(_.close())
      }
    }))

    result
  }
}