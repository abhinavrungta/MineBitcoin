package old.main

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.event.LoggingReceive

object Master {
  case class StartMining(account: ActorRef, amount: BigInt) {
  }
}

class Master extends Actor {
  import Master._

  var results: Map[String, String] = Map()

  def receive = LoggingReceive {
    case Worker.ValidBitCoin(inputStr: String, outputStr: String) => results += (inputStr -> outputStr)
    case Worker.Done => sender ! Worker.Compute("AB", 1)
    case StartMining =>
    case _ =>
    //case BankAccount.Done => println("Deposit Done")
  }
}

object Project1 {
  def main(args: Array[String]) {
    var noOfActors = 2 // no of Worker Actors to create on invoking the program.
    var leadingZeros = 0
    var ipAddress = ""
    // exit if argument not passed as command line param
    if (args.length < 1) {
      println("Invalid no of args")
      System.exit(1)
    } // check if argument passed is the ipAddress or the LeadingZero
    else {
      args(0) match {
        case s: String if s.contains(".") =>
          ipAddress = s
          println(ipAddress)
        case s: String =>
          leadingZeros = s.toInt
          println(leadingZeros)
        case _ => System.exit(1)
      }
    }
    val system = ActorSystem("BitCoinSystem")
    val actorA = system.actorOf(Props(classOf[Worker], leadingZeros))

  }
}
