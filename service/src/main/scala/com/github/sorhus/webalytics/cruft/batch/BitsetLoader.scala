//package com.github.sorhus.webalytics.cruft.batch
//
//import java.io._
//import java.nio.MappedByteBuffer
//import java.nio.channels.FileChannel.MapMode
//import java.util.UUID
//import java.util.concurrent.TimeUnit
//
//import akka.actor.{ActorRef, ActorSystem}
//import com.github.sorhus.webalytics.akka.model._
//import com.github.sorhus.webalytics.cruft.model._
//import org.json4s.{DefaultFormats, Formats}
//import org.json4s.jackson.JsonMethods._
//import org.roaringbitmap.buffer.ImmutableRoaringBitmap
//import org.slf4j.LoggerFactory
//import akka.pattern.ask
//import akka.util.Timeout
//import com.github.sorhus.webalytics.akka.document.DocumentIdActor
//import com.github.sorhus.webalytics.akka.domain.DomainActor
//import com.github.sorhus.webalytics.akka.segment.{Bitset => _, _}
//
//import scala.collection.mutable
//import scala.concurrent.{Await, Future}
//import scala.concurrent.duration.Duration
//
//
///**
//  * Batch load data into a bitset.
//  *
//  *
//  * Use cases:
//  *   1. We need to be able to update elements and add new elements
//  *   2. We don't need to update elements, but we need to be able to add elements
//  *   3. We don't need to update nor add elements
//  *
//  * Element ids can be attached or not attached.
//  *
//  * We always need to store meta data regarding dimensions and values
//  * Only in case of 1) do we need to store (element id -> document id) mapping
//  * In case of 3) we need no additional meta data
//  */
//object BitsetLoader extends App {
//
//  val log = LoggerFactory.getLogger(getClass)
//  val inputFile = args(0)
//  val bucket = Bucket(args(1))
//  val outputDir = args(2)
//
//  val in = new BufferedReader(new FileReader(inputFile))
//  val out = new BufferedWriter(new OutputStreamWriter(System.out))
////  val audienceDao = new BitsetDao[RoaringBitmap](new RoaringBitmapWrapper().create _)
//
//  implicit val system = ActorSystem("webalytics-bitset-loader")
//  implicit val timeOut = Timeout(10, TimeUnit.MINUTES)
//
//  import scala.concurrent.ExecutionContext.Implicits.global
//
////  val metaDao = Try(args(3)).toOption match {
////    case Some("devnull") => new DevNullMetaDao
////    case _ => new DelayedBatchInsertMetaDao(new RedisMetaDao())
////  }
//  val audienceActor: ActorRef = system.actorOf(SegmentActor.props(null), "audience")
//  val queryActor: ActorRef = system.actorOf(DomainActor.props(audienceActor), "meta")
//  val documentActor: ActorRef = system.actorOf(DocumentIdActor.props(audienceActor, queryActor, 0), "document")
//
//  val loader = new BitsetLoader()
////  loader.load(bucket, in, audienceDao)(metaDao)
//  val loaded: Stream[Future[Any]] = loader.load(bucket, in, documentActor)
//  loaded.foreach(l => Await.result(l, Duration.Inf))
//  log.info("Loaded all elements")
////  Try(metaDao.asInstanceOf[DelayedBatchInsertMetaDao]).map(_.commit())
////  Await.result(system.terminate(), Duration.Inf)
////  loader.write(outputDir, audienceDao)
//
//  documentActor ! SaveSnapshot
//  queryActor ! SaveSnapshot
//  audienceActor ! SaveSnapshot
////  log.info("Writing to disk")
////  val write = (audienceActor ? "getall").map{
////    case s: BitsetState[RoaringBitmap] =>
////      loader.write(outputDir, s)
////  }
////  Await.result(write, Duration.Inf)
//
//  Await.result(documentActor ? Shutdown, Duration.Inf)
//  Await.result(queryActor ? Shutdown, Duration.Inf)
//  Await.result(audienceActor ? Shutdown, Duration.Inf)
//
////  Thread.sleep(TimeUnit.MINUTES.toMillis(1))
//  Await.result(system.terminate(), Duration.Inf)
//}
//class BitsetLoader {
//
//  val log = LoggerFactory.getLogger(getClass)
//
//  implicit val jsonFormats: Formats = DefaultFormats
//
////  def load(bucket: Bucket, in: BufferedReader, audienceDao: BitsetDao[RoaringBitmap])(implicit metaDao: MetaDao): Unit = {
//  def load(bucket: Bucket, in: BufferedReader, postActor: ActorRef)(implicit timeout: Timeout): Stream[Future[Any]] = {
//    Stream.continually(in.readLine())
//      .takeWhile(_ != null)
//      .map(_.split("\t"))
//      .map {
//        case Array(elementId, json) =>
//          val element = parse(json)
//            .extract[Map[String, List[String]]]
//            .map { case (dimension: String, values: List[String]) =>
//              Dimension(dimension) -> values.map(v => Value(v)).toSet
//            }
//          (ElementId(elementId), Element(element))
//        case Array(json) =>
//          val element = parse(json)
//            .extract[Map[String, List[String]]]
//            .map { case (dimension: String, values: List[String]) =>
//              Dimension(dimension) -> values.map(v => Value(v)).toSet
//            }
//          (ElementId(UUID.randomUUID().toString), Element(element))
//      }
//      .map { case (elementId, element) =>
//        postActor ? PostCommand(bucket, elementId, element)
////        audienceDao.post(bucket, elementId, element)
//      }
//  }
//
//  def write(path: String, state: MutableSegmentState) = {
//    state.bitsets.foreach{case(bucket, dimvals) =>
//      dimvals.foreach{case(dimension, values) =>
//        val file = new File(s"$path/${bucket.b}/${dimension.d}")
//        file.getParentFile.mkdirs()
//        file.createNewFile()
//        val fos = new FileOutputStream(file)
//        val dos = new DataOutputStream(fos)
//        val bytes = 0
//        values.toList.sortBy(_._1.v).foreach{case(value, bitset) =>
//          bitset.impl().runOptimize()
//          log.info("Writing bitset: {} with bytes: ", (bucket.b, dimension.d, value.v, bitset.cardinality(), bitset.impl().serializedSizeInBytes()))
//          bitset.impl().serialize(dos)
//        }
//        log.info("Closing outputstream for {}", dimension)
//        dos.close()
//      }
//    }
//  }
//
//  def read(path: String, metaActor: ActorRef)(implicit timout: Timeout): Map[Bucket, Map[Dimension, Map[Value, Bitset[ImmutableRoaringBitmap]]]] = {
//    val files = mutable.Set[RandomAccessFile]()
//    val result = new File(path).list().map { bucket =>
//      val dimensions = new File(s"$path/$bucket").list().toList.map(Dimension.apply)
//      log.info("Listing bucket dir, found dimensions: {}", dimensions)
//      val space = Await.result(metaActor ? GetAll, Duration.Inf).asInstanceOf[Element]
//      log.info("Asked and received space {}", space)
//      Bucket(bucket) -> space.e.map{ case(dimension, values) =>
//        val name = s"$path/$bucket/${dimension.d}"
//        val file = new RandomAccessFile(name, "r")
//        files.add(file)
//        log.info("Memory mapping file {}", s"$name: ${file.length()}")
//        val memoryMapped: MappedByteBuffer = file.getChannel.map(MapMode.READ_ONLY, 0, file.length())
//        val bb = memoryMapped.slice()
//        log.info(s"got bytebuffer {}", bb)
//        dimension -> values.toList.sortBy(_.v).map{value =>
//          val bitset = new ImmutableRoaringBitmap(bb)
//          log.info("Read bitset: {}", (bucket, dimension.d, value.v, bitset.getCardinality))
//          log.info("At position: {}", (bb.position(), bitset.serializedSizeInBytes()))
//          bb.position(bb.position() + bitset.serializedSizeInBytes())
//          value -> ImmutableBitMap.cons(bitset)
//        }.toMap
//      }.toMap
//    }.toMap
//
//    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
//      override def run(): Unit = {
//        files.foreach(_.close())
//      }
//    }))
//
////    result
//    null
//  }
//}
