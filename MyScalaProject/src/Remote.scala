import akka.actor.Actor
import akka.actor.ActorRef
import Project1.Master
import akka.actor.ActorSystem
import akka.actor.Props
import Project1.Worker
import com.typesafe.config.ConfigFactory

object Remote {
  def main(args: Array[String]) {
    var leadingZeros = 4
    var ipAddress = ""
    var port = 10000

    if (args.length < 1) {
      println("Invalid no of args")
      System.exit(1)
    } else {
      args(0) match {
        case s: String if s.contains(".") =>
          ipAddress = s
        case s: String =>
          leadingZeros = s.toInt
        case _ => System.exit(1)
      }
    }
    var config = ConfigFactory.parseString("""
    akka {
    actor {
      provider = "akka.remote.RemoteActorRefProvider"
    }
    remote {
      transport = "akka.remote.netty.NettyRemoteTransport"
      netty {
        hostname = "localhost"
        port = """ + port + """
      }
    }
  }""")
    val system = ActorSystem("RemoteBitCoinSystem", ConfigFactory.load(config))
    val worker = system.actorOf(Props(classOf[Worker], leadingZeros), name = "Worker")

    val master = system.actorSelection("akka.tcp://BitCoinSystem@" + ipAddress + ":12000/user/Master")
    master ! Master.NewRemoteWorker

  }

}