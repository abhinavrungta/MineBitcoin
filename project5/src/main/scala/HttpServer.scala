import akka.actor.{ ActorSystem, Props }
import scala.concurrent.duration._
import akka.util.Timeout
import akka.actor._
import spray.http._
import spray.can.Http
import HttpMethods._
import akka.io.IO
import akka.pattern.ask
import spray.json.DefaultJsonProtocol

object HttpServer {

  def main(args: Array[String]) {
    implicit val system = ActorSystem()

    var ipAddress = ""
    val server = system.actorSelection("akka.tcp://TwitterServer@" + ipAddress + ":12000/user/Watcher/Router")

    // the handler actor replies to incoming HttpRequests
    val handler = system.actorOf(Props(new DemoService(server)), name = "handler")

    IO(Http) ! Http.Bind(handler, interface = "localhost", port = 10000)

  }

  case class RecvTimeline(tweets: Map[Int, String])
  case class SendTweet(userId: Int, time: Long, msg: String)

  object myJson extends DefaultJsonProtocol {
    implicit val tweetFormat = jsonFormat3(SendTweet)
    implicit val timelineFormat = jsonFormat1(RecvTimeline)
  }

  class DemoService(server: ActorSelection) extends Actor {
    import myJson._
    implicit val timeout: Timeout = 1.second // for the actor 'asks'
    import context.dispatcher // ExecutionContext for the futures and scheduler

    def receive = {
      // when a new connection comes in we register ourselves as the connection handler
      case _: Http.Connected => sender ! Http.Register(self)

      case HttpRequest(GET, Uri.Path(path), _, _, _) if path startsWith "/timeline" =>
        var id = path.split("/").last.toInt
        var client = sender
        val result = ask(server, Project4Server.Server.SendTimeline(id)).mapTo[Map[Int, String]]
        result onSuccess {
          case result: Map[Int, String] => client ! (RecvTimeline(result))
        }

      case HttpRequest(POST, Uri.Path("/tweet"), headers, entity: HttpEntity.NonEmpty, protocol) =>
        val tweet = entity.asInstanceOf[SendTweet]
        var client = sender
        val result = ask(server, tweet)
        result onComplete {
          case result => client ! "OK"
        }

      case Timedout(HttpRequest(method, uri, _, _, _)) =>
        sender ! HttpResponse(
          status = 500,
          entity = "The " + method + " request to '" + uri + "' has timed out...")

      case _: HttpRequest => sender ! HttpResponse(status = 404, entity = "Unknown resource!")
    }
  }
}