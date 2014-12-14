package main.scala

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
import spray.json.pimpAny
import spray.json.pimpString

object HttpServer extends JsonFormats {

  def main(args: Array[String]) {
    if (args.length < 1) {
      println("INVALID NO OF ARGS.  USAGE :")
      System.exit(1)
    } else if (args.length == 1) {
      implicit val system = ActorSystem("HTTPServer", ConfigFactory.load(ConfigFactory.parseString("""{ "akka" : { "actor" : { "provider" : "akka.remote.RemoteActorRefProvider" }, "remote" : { "enabled-transports" : [ "akka.remote.netty.tcp" ], "netty" : { "tcp" : { "port" : 11000 , "maximum-frame-size" : 12800000b } } } } } """)))

      var privateIp = args(0)
      val server = system.actorSelection("akka.tcp://TwitterServer@" + privateIp + ":12000/user/Watcher/Router")

      // the handler actor replies to incoming HttpRequests
      val handler = system.actorOf(Props(new HttpService(server)), name = "handler")

      val ipAddress = InetAddress.getLocalHost.getHostAddress()
      implicit val timeout: Timeout = 10.second // for the actor 'asks'
      IO(Http) ? Http.Bind(handler, interface = ipAddress, port = 8080)
    }
  }

  class HttpService(server: ActorSelection) extends Actor {
    implicit val timeout: Timeout = 5.second // for the actor 'asks'
    import context.dispatcher // ExecutionContext for the futures and scheduler

    def receive = {
      // when a new connection comes in we register ourselves as the connection handler
      case _: Http.Connected => sender ! Http.Register(self)

      case HttpRequest(GET, Uri.Path("/"), _, _, _) =>
        val body = HttpEntity(ContentTypes.`application/json`, "OK")
        sender ! HttpResponse(entity = body)

      case HttpRequest(POST, Uri.Path("/tweet"), _, entity: HttpEntity.NonEmpty, _) =>
        val tweet = entity.data.asString.parseJson.convertTo[SendTweet]
        var client = sender
        val result = (server ? Project4Server.Server.AddTweet(tweet.userId, tweet.time, tweet.msg)).mapTo[String]
        result onSuccess {
          case result =>
            client ! HttpResponse(entity = result)
        }

      case HttpRequest(GET, Uri.Path(path), _, _, _) if path startsWith "/home_timeline" =>
        var id = path.split("/").last.toInt
        var client = sender
        val result = (server ? Project4Server.Server.SendHomeTimeline(id)).mapTo[List[Project4Server.Tweets]]
        result onSuccess {
          case result: List[Project4Server.Tweets] =>
            val body = HttpEntity(ContentTypes.`application/json`, result.toJson.toString)
            client ! HttpResponse(entity = body)
        }

      case HttpRequest(GET, Uri.Path(path), _, _, _) if path startsWith "/user_timeline" =>
        var id = path.split("/").last.toInt
        var client = sender
        val result = (server ? Project4Server.Server.SendUserTimeline(id)).mapTo[List[Project4Server.Tweets]]
        result onSuccess {
          case result: List[Project4Server.Tweets] =>
            val body = HttpEntity(ContentTypes.`application/json`, result.toJson.toString)
            client ! HttpResponse(entity = body)
        }

      case HttpRequest(GET, Uri.Path(path), _, _, _) if path startsWith "/mentions" =>
        var id = path.split("/").last.toInt
        var client = sender
        val result = (server ? Project4Server.Server.SendMentions(id)).mapTo[List[Project4Server.Tweets]]
        result onSuccess {
          case result: List[Project4Server.Tweets] =>
            val body = HttpEntity(ContentTypes.`application/json`, result.toJson.toString)
            client ! HttpResponse(entity = body)
        }

      case HttpRequest(GET, Uri.Path(path), _, _, _) if path startsWith "/user" =>
        var id = path.split("/").last.toInt
        var client = sender
        val result = (server ? Project4Server.Server.SendUserProfile(id)).mapTo[List[UserProfile]]
        result onSuccess {
          case result: List[UserProfile] =>
            val body = HttpEntity(ContentTypes.`application/json`, result.toJson.toString)
            client ! HttpResponse(entity = body)
        }

      case HttpRequest(GET, Uri.Path(path), _, _, _) if path startsWith "/followers" =>
        var id = path.split("/").last.toInt
        var client = sender
        val result = (server ? Project4Server.Server.SendFollowers(id)).mapTo[List[UserProfile]]
        result onSuccess {
          case result: List[UserProfile] =>
            val body = HttpEntity(ContentTypes.`application/json`, result.toJson.toString)
            client ! HttpResponse(entity = body)
        }

      case HttpRequest(GET, Uri.Path(path), _, _, _) if path startsWith "/following" =>
        var id = path.split("/").last.toInt
        var client = sender
        val result = (server ? Project4Server.Server.SendFollowing(id)).mapTo[List[UserProfile]]
        result onSuccess {
          case result: List[UserProfile] =>
            val body = HttpEntity(ContentTypes.`application/json`, result.toJson.toString)
            client ! HttpResponse(entity = body)
        }

      case HttpRequest(POST, Uri.Path("/msg"), _, entity: HttpEntity.NonEmpty, _) =>
        val tweet = entity.data.asString.parseJson.convertTo[SendMsg]
        var client = sender
        val result = (server ? Project4Server.Server.AddMsg(tweet.senderId, tweet.time, tweet.msg, tweet.recepientId)).mapTo[String]
        result onSuccess {
          case result =>
            client ! HttpResponse(entity = result)
        }

      case HttpRequest(GET, Uri.Path(path), _, _, _) if path startsWith "/msg" =>
        var id = path.split("/").last.toInt
        var client = sender
        val result = (server ? Project4Server.Server.SendMessages(id)).mapTo[List[Project4Server.Messages]]
        result onSuccess {
          case result: List[Project4Server.Messages] =>
            val body = HttpEntity(ContentTypes.`application/json`, result.toJson.toString)
            client ! HttpResponse(entity = body)
        }

      case _: HttpRequest => sender ! HttpResponse(status = 404, entity = "Unknown!")

    }
  }
}