import javax.servlet.ServletContext
import akka.actor.ActorSystem
import com.github.sorhus.webalytics.post.Servlet
import org.scalatra.LifeCycle

class ScalatraBootstrap extends LifeCycle {

  implicit val system = ActorSystem("webalytics-api")

  override def init(context: ServletContext) {
    context.mount(new Servlet(), "/")
  }

  override def destroy(context:ServletContext) {
    system.shutdown()
  }

}
