import javax.servlet.ServletContext

import akka.actor.ActorSystem
import com.github.sorhus.webalytics.api.Servlet
import com.github.sorhus.webalytics.batch.BitsetLoader
import com.github.sorhus.webalytics.impl.RoaringBitmapWrapper
import com.github.sorhus.webalytics.impl.redis.RedisDao
import com.github.sorhus.webalytics.model._
import org.roaringbitmap.RoaringBitmap
import org.roaringbitmap.buffer.ImmutableRoaringBitmap
import org.scalatra.LifeCycle

class ScalatraBootstrap extends LifeCycle {

  implicit val system = ActorSystem("webalytics-api")

  override def init(context: ServletContext) {
    val bitsetsDir = context.getAttribute("bitsets").toString
    val dao = if(bitsetsDir != null) {
      val audienceDao = new BitsetDao[RoaringBitmap](new RoaringBitmapWrapper().create _)
      val loader = new BitsetLoader()
      val bitsets: Map[Bucket, Map[Dimension, Map[Value, Bitset[ImmutableRoaringBitmap]]]] = loader.read(bitsetsDir)
      new ImmutableBitsetDao(bitsets)
    } else {
      new RedisDao()
    }
    context.mount(new Servlet(dao), "/")
  }

  override def destroy(context:ServletContext) {
    system.shutdown()
  }

}
