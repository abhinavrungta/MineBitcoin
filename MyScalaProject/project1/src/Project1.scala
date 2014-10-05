package src.project1

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.event.LoggingReceive
import scala.util.control.Breaks
import java.security.MessageDigest
import com.typesafe.config.ConfigFactory
import scala.collection.mutable.ArrayBuffer
import akka.actor.Terminated
import akka.actor.actorRef2Scala

object Project1 {
  def main(args: Array[String]) {
    var noOfActors = 10 // no of Worker Actors to create on invoking the program.
    var leadingZeros = 4
    var ipAddress = ""
    var blockSize = 100000
    var threshold = 10000000

    // exit if argument not passed as command line param
    if (args.length < 1) {
      println("Invalid no of args")
      System.exit(1)
    } else if (args.length == 1) {
      args(0) match {
        case s: String if s.contains(".") =>
          ipAddress = s
          val remoteSystem = ActorSystem("RemoteBitCoinSystem", ConfigFactory.load(ConfigFactory.parseString("""{ "akka" : { "actor" : { "provider" : "akka.remote.RemoteActorRefProvider" }, "remote" : { "enabled-transports" : [ "akka.remote.netty.tcp" ], "netty" : { "tcp" : { "port" : 13000 } } } } } """)))
          val worker = remoteSystem.actorOf(Props(new Worker()), name = "Worker")
          val watcher = remoteSystem.actorOf(Props(new Watcher()), name = "Watcher")

          watcher ! Watcher.WatchMe(worker)

          val master = remoteSystem.actorSelection("akka.tcp://BitCoinSystem@" + ipAddress + ":12000/user/Master")
          master.tell(Master.NewRemoteWorker, worker)

        case s: String =>
          leadingZeros = s.toInt

          val system = ActorSystem.create("BitCoinSystem", ConfigFactory.load(ConfigFactory.parseString("""{ "akka" : { "actor" : { "provider" : "akka.remote.RemoteActorRefProvider" }, "remote" : { "enabled-transports" : [ "akka.remote.netty.tcp" ], "netty" : { "tcp" : { "port" : 12000 } } } } } """)))
          val master = system.actorOf(Props(new Master(leadingZeros, blockSize, noOfActors, threshold)), name = "Master")
          master.tell(Master.StartMining, master)

        case _ => System.exit(1)
      }
    }

  }

  object Watcher {
    // Used by others to register an Actor for watching
    case class WatchMe(ref: ActorRef)
  }

  class Watcher extends Actor {
    import Watcher._

    // Keep track of what we're watching
    val watched = ArrayBuffer.empty[ActorRef]

    // Watch and check for termination
    final def receive = {
      case WatchMe(ref) =>
        context.watch(ref)
        watched += ref
      case Terminated(ref) =>
        watched -= ref
        if (watched.isEmpty) {
          context.system.shutdown
        }
    }
  }

  object StringIncrementer {
    def next(s: String): String = {
      val length = s.length
      var c = s.charAt(length - 1)
      if (c == 'Z') return if (length > 1) next(s.substring(0, length - 1)) + 'A' else "AA"
      s.substring(0, length - 1) + (c.toInt + 1).toChar
    }

    def incrementByN(s: String, n: Int): String = {
      var i = 0
      var temp = s
      for (i <- 1 to n) {
        temp = next(temp)
      }
      return temp
    }
  }

  object SHA256 {

    def hash(str: String): String = {
      MessageDigest.getInstance("SHA-256").digest(str.getBytes).foldLeft("")((s: String, b: Byte) => s +
        Character.forDigit((b & 0xf0) >> 4, 16) +
        Character.forDigit(b & 0x0f, 16))
    }
  }

  object Worker {
    case class Compute(str: String, ctr: Int, zeros: Int)
    case object Done
    case object Failed
    case object Stop
    case object StopAck
  }

  class Worker() extends Actor {
    import Worker._
    var tempStartString = ""
    private val prefix = "asrivastava"

    def receive = LoggingReceive {
      case Compute(str, ctr, zeros) =>
        ComputeImpl(str, ctr, zeros, sender)
        sender ! Done
      case Stop =>
        sender ! StopAck
        context.stop(self)
      case _ => sender ! Failed
    }

    def ComputeImpl(str: String, ctr: Int, zeros: Int, sender: ActorRef) {
      var i = 1
      tempStartString = str
      for (i <- 1 to ctr) {
        var hex = SHA256.hash(prefix + tempStartString)
        var result = checkLeadingZeros(hex, zeros)
        if (result) {
          sender ! Master.ValidBitCoin(prefix + tempStartString, hex)
        }
        tempStartString = StringIncrementer.next(tempStartString)
      }
    }

    def checkLeadingZeros(s: String, numberOfZeros: Int): Boolean = {
      var a = 0
      var flag = true
      val loop = new Breaks;
      loop.breakable {
        for (i <- 0 to numberOfZeros - 1) {
          if (s.charAt(i) == '0') {
            flag = true
          } else {
            flag = false
            loop.break
          }
        }
      }
      if (flag == false) {
        return false
      } else return true
    }
  }

  object Master {
    case class ValidBitCoin(inputStr: String, outputStr: String)
    case object StartMining
    case object Stop
    case object NewRemoteWorker
  }

  class Master(leadingZeros: Int, blockSize: Int, noOfActors: Int, threshold: Int) extends Actor {
    import Master._
    var results: Map[String, String] = Map()
    var currentInputString = "A"
    var actorPool = new Array[ActorRef](noOfActors)
    var inputCtr = 0
    var stoppedActors = 0
    var actors = noOfActors

    def receive = LoggingReceive {
      case ValidBitCoin(inputStr: String, outputStr: String) =>
        results += (inputStr -> outputStr)
        println(inputStr + "\t" + outputStr)

      case Worker.Done =>
        if (inputCtr < threshold) {
          sender ! Worker.Compute(currentInputString, blockSize, leadingZeros)
          currentInputString = StringIncrementer.incrementByN(currentInputString, blockSize)
          inputCtr += blockSize
        } else {
          sender ! Worker.Stop
        }
      case StartMining => startMiningImpl()

      case Worker.StopAck =>
        stoppedActors += 1
        if (stoppedActors == actors) {
          context.system.shutdown
        }

      case NewRemoteWorker =>
        if (inputCtr < threshold) {
          actors += 1
          sender ! Worker.Compute(currentInputString, blockSize, leadingZeros)
          currentInputString = StringIncrementer.incrementByN(currentInputString, blockSize)
          inputCtr += blockSize
        } else {
          sender ! Worker.Stop
        }
      case _ =>
        println("FAILED IN MASTER RECV.")
    }

    def startMiningImpl() {
      var i = 1
      for (i <- 1 to noOfActors) {
        actorPool(i - 1) = context.actorOf(Props(classOf[Worker]), name = "Worker" + i)
      }
      for (i <- 1 to noOfActors) {
        actorPool(i - 1) ! Worker.Compute(currentInputString, blockSize, leadingZeros)
        currentInputString = StringIncrementer.incrementByN(currentInputString, blockSize)
        inputCtr += blockSize
      }
    }

  }
}