import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.event.LoggingReceive
import scala.util.control.Breaks
import java.security.MessageDigest

object Project1 {
  def main(args: Array[String]) {
    var noOfActors = 10 // no of Worker Actors to create on invoking the program.
    var leadingZeros = 4
    var ipAddress = ""
    var blockSize = 10000
    var threshold = 1000000

    // exit if argument not passed as command line param
    if (args.length < 4) {
      println("Invalid no of args")
      System.exit(1)
    } // check if argument passed is the ipAddress or the LeadingZero
    else {
      leadingZeros = args(0).toInt
      noOfActors = args(1).toInt
      blockSize = args(2).toInt
      threshold = args(3).toInt
      //      args(0) match {
      //        case s: String if s.contains(".") =>
      //          ipAddress = s
      //        case s: String =>
      //          leadingZeros = s.toInt
      //        case _ => System.exit(1)
      //      }
    }
    val system = ActorSystem("BitCoinSystem")
    val master = system.actorOf(Props(classOf[Master], leadingZeros, blockSize, noOfActors, threshold), name = "Master")
    master ! Master.StartMining

  }

  object StringIncrementer {
    def next(s: String): String = {
      val length = s.length
      var c = s.charAt(length - 1)
      if (c == 'z' || c == 'Z') return if (length > 1) next(s.substring(0, length - 1)) + 'A' else "AA"
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
    case class Compute(str: String, ctr: Int)
    case object Done
    case object Failed
    case object Stop
    case object StopAck
  }

  class Worker(leadingZeros: Int) extends Actor {
    import Worker._
    var tempStartString = ""
    private val prefix = "asrivastava"

    def receive = LoggingReceive {
      case Compute(str, ctr) =>
        ComputeImpl(str, ctr, sender)
        sender ! Done
      case Stop =>
        sender ! StopAck
        context.stop(self)
      case _ => sender ! Failed
    }

    def ComputeImpl(str: String, ctr: Int, sender: ActorRef) {
      var i = 1
      tempStartString = str
      for (i <- 1 to ctr) {
        var hex = SHA256.hash(prefix + tempStartString)
        var result = checkLeadingZeros(hex, leadingZeros)
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
          sender ! Worker.Compute(currentInputString, blockSize)
          currentInputString = StringIncrementer.incrementByN(currentInputString, blockSize)
          inputCtr += blockSize
        } else
          sender ! Worker.Stop
      case StartMining => startMiningImpl()

      case Worker.StopAck =>
        stoppedActors += 1
        if (stoppedActors == actors) {
          context.system.shutdown
        }

      case NewRemoteWorker =>
        if (inputCtr < threshold) {
          actors += 1
          sender ! Worker.Compute(currentInputString, blockSize)
          currentInputString = StringIncrementer.incrementByN(currentInputString, blockSize)
          inputCtr += blockSize
        } else
          sender ! Worker.Stop

      case _ =>
        println("FAILED IN MASTER RECV.")
    }

    def startMiningImpl() {
      var i = 1
      for (i <- 1 to noOfActors) {
        actorPool(i - 1) = context.actorOf(Props(classOf[Worker], leadingZeros), name = "Worker" + i)
      }
      for (i <- 1 to noOfActors) {
        actorPool(i - 1) ! Worker.Compute(currentInputString, blockSize)
        currentInputString = StringIncrementer.incrementByN(currentInputString, blockSize)
        inputCtr += blockSize
      }
    }

  }
}