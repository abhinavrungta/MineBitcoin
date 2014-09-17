import akka.actor.Actor
import akka.actor.ActorRef
import Project1.Master
import akka.actor.ActorSystem
import akka.actor.Props
import Project1.Worker

object Remote {
  def main(args: Array[String]) {
    var leadingZeros = 4
    var ipAddress = ""

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
    val system = ActorSystem("RemoteBitCoinSystem")
    val worker = system.actorOf(Props(classOf[Worker], leadingZeros), name = "Worker")

    val master = system.actorSelection("akka://BitCoinSystem@127.0.0.1:2552/user/Master")
    master ! Master.NewRemoteWorker

  }

}