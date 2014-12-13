import java.net.InetAddress
import scala.concurrent.duration.DurationInt
import com.typesafe.config.ConfigFactory
import akka.actor.Actor
import akka.actor.ActorSelection
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.actorRef2Scala
import akka.io.IO
import akka.pattern.ask
import akka.util.Timeout
import akka.util.Timeout.durationToTimeout
import spray.can.Http
import spray.http.ContentTypes
import spray.http.HttpEntity
import spray.http.HttpEntity.apply
import spray.http.HttpMethods.GET
import spray.http.HttpMethods.POST
import spray.http.HttpRequest
import spray.http.HttpResponse
import spray.http.StatusCode.int2StatusCode
import spray.http.Uri
import spray.json._
import scala.collection.mutable.ArrayBuffer

object HttpServer {

  def main(args: Array[String]) {
    if (args.length < 1) {
      println("INVALID NO OF ARGS.  USAGE :")
      System.exit(1)
    } else if (args.length == 1) {
      implicit val system = ActorSystem("HTTPServer", ConfigFactory.load(ConfigFactory.parseString("""{ "akka" : { "actor" : { "provider" : "akka.remote.RemoteActorRefProvider" }, "remote" : { "enabled-transports" : [ "akka.remote.netty.tcp" ], "netty" : { "tcp" : { "port" : 11000 , "maximum-frame-size" : 1280000b } } } } } """)))

      var privateIp = args(0)
      val server = system.actorSelection("akka.tcp://TwitterServer@" + privateIp + ":12000/user/Watcher/Router")

      // the handler actor replies to incoming HttpRequests
      val handler = system.actorOf(Props(new DemoService(server)), name = "handler")

      val ipAddress = InetAddress.getLocalHost.getHostAddress()
      implicit val timeout: Timeout = 10.second // for the actor 'asks'
      IO(Http) ? Http.Bind(handler, interface = ipAddress, port = 8080)
    }
  }

  case class SendTweet(userId: Int, time: Long, msg: String)

  object myJson extends DefaultJsonProtocol {
    implicit val tweetFormat = jsonFormat3(SendTweet)
    implicit object TimelineJsonFormat extends JsonFormat[Project4Server.Tweets] {
      def write(c: Project4Server.Tweets) = JsObject(
        "authorId" -> JsNumber(c.authorId),
        "message" -> JsString(c.message),
        "timeStamp" -> JsString(c.timeStamp.toString),
        "tweetId" -> JsString(c.tweetId),
        "mentions" -> JsArray(c.mentions.map(_.toJson).toVector),
        "hashTags" -> JsArray(c.hashtags.map(_.toJson).toVector))

      def read(value: JsValue) = {
        value.asJsObject.getFields("tweetId", "authorId", "message", "timeStamp", "mentions", "hashTags") match {
          case Seq(JsString(tweetId), JsNumber(authorId), JsString(message), JsString(timeStamp), JsArray(mentions), JsArray(hashTags)) =>
            new Project4Server.Tweets(tweetId, authorId.toInt, message, timeStamp.toLong, mentions.to[ArrayBuffer].map(a => a.convertTo[String]), hashTags.to[ArrayBuffer].map(a => a.convertTo[String]))
          case _ => throw new DeserializationException("Tweets expected")
        }
      }
    }
  }

  class DemoService(server: ActorSelection) extends Actor {
    import myJson._
    implicit val timeout: Timeout = 5.second // for the actor 'asks'
    import context.dispatcher // ExecutionContext for the futures and scheduler

    def receive = {
      // when a new connection comes in we register ourselves as the connection handler
      case _: Http.Connected => sender ! Http.Register(self)

      case HttpRequest(GET, Uri.Path("/"), _, _, _) =>
        val body = HttpEntity(ContentTypes.`application/json`, ("OK").toJson.toString)
        sender ! HttpResponse(entity = body)

      case HttpRequest(GET, Uri.Path(path), _, _, _) if path startsWith "/timeline" =>
        var id = path.split("/").last.toInt
        var client = sender
        val result = (server ? Project4Server.Server.SendTimeline(id)).mapTo[List[Project4Server.Tweets]]
        result onSuccess {
          case result: List[Project4Server.Tweets] =>
            val body = HttpEntity(ContentTypes.`application/json`, result.toJson.toString)
            client ! HttpResponse(entity = body)
        }

      case HttpRequest(POST, Uri.Path("/tweet"), _, entity: HttpEntity.NonEmpty, _) =>
        val tweet = entity.data.asString.parseJson.convertTo[SendTweet]
        var client = sender
        val result = server ? Project4Server.Server.AddTweet(tweet.userId, tweet.time, tweet.msg)
        result onComplete {
          case result =>
            client ! HttpResponse(entity = "OK")
        }

      case _: HttpRequest => sender ! HttpResponse(status = 404, entity = "Unknown!")

    }
  }
}