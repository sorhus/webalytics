import javax.servlet.ServletContext

import akka.actor.ActorSystem
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
    implicit val metaDao = new RedisMetaDao()
    val dao = if(bitsetsDir != null) {
      log.info("Loading bitsets from {}", bitsetsDir)
      val loader = new BitsetLoader()
      val bitsets: Map[Bucket, Map[Dimension, Map[Value, Bitset[ImmutableRoaringBitmap]]]] = loader.read(bitsetsDir)
      new ImmutableBitsetDao(bitsets)
    } else {
      log.info("Using RedisDao")
      new RedisDao()
    }
    context.mount(new Servlet(dao), "/")
  }

  override def destroy(context:ServletContext) {
    system.shutdown()
  }

}
