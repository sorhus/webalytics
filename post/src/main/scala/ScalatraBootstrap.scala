import java.util.concurrent.TimeUnit
import javax.servlet.ServletContext

import akka.actor.{ActorRef, ActorSystem}
import akka.util.Timeout
import com.github.sorhus.webalytics.akka.{BitsetAudienceActor, DimensionValueActor}
import com.github.sorhus.webalytics.api.Servlet
import com.github.sorhus.webalytics.batch.BitsetLoader
import com.github.sorhus.webalytics.impl.redis.{RedisDao, RedisMetaDao}
import com.github.sorhus.webalytics.model._
import org.roaringbitmap.buffer.ImmutableRoaringBitmap
import org.scalatra.LifeCycle
import org.slf4j.LoggerFactory

class ScalatraBootstrap extends LifeCycle {

  val log = LoggerFactory.getLogger(getClass)

  implicit val system = ActorSystem("webalytics-api")

  override def init(context: ServletContext) {
    val bitsetsDir = context.getAttribute("bitsets").toString
    implicit val timeOut = Timeout(10, TimeUnit.MINUTES)

    val dao = if(bitsetsDir != null) {
      log.info("Loading bitsets from {}", bitsetsDir)
      val loader = new BitsetLoader()
      val audienceActor: ActorRef = system.actorOf(BitsetAudienceActor.props(), "audience")
      val queryActor: ActorRef = system.actorOf(DimensionValueActor.props(audienceActor), "meta")
      val bitsets: Map[Bucket, Map[Dimension, Map[Value, Bitset[ImmutableRoaringBitmap]]]] = loader.read(bitsetsDir, queryActor)
      new ImmutableBitsetDao(bitsets)
    } else {
      log.info("Using RedisDao")
      new RedisDao()
    }
    implicit val metaDao = new RedisMetaDao()
    context.mount(new Servlet(dao), "/")
  }

  override def destroy(context:ServletContext) {
    system.shutdown()
  }

}
