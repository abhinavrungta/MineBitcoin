package project3.src;
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.DurationInt
import scala.util.Random

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Cancellable
import akka.actor.Props
import akka.actor.actorRef2Scala
import akka.event.LoggingReceive
import java.security.MessageDigest

// Think of tracking the messages being recd by the actors and shutdown entire system when recd atleast once.

object Project3 {
  def main(args: Array[String]) {
    // exit if arguments not passed as command line param.
    if (args.length < 2) {
      println("INVALID NO OF ARGS.  USAGE :")
      println("1. # of Nodes")
      println("2. # of Requests")
      System.exit(1)
    } else if (args.length == 2) {
      var numNodes = args(0).toInt
      var numRequests = args(1).toInt

      // create actor system and a watcher actor
      val system = ActorSystem("Pastry")
      val watcher = system.actorOf(Props(new Watcher(numNodes, numRequests)), name = "Watcher")
    }
  }

  object Watcher {
    case class Terminate(ref: ActorRef, sw: Double = 0.0)
    case object Initiate
  }

  class Watcher(noOfNodes: Int, noOfRequests: Int) extends Actor {
    import Watcher._
    import context._
    var startTime = System.currentTimeMillis()

    // keep track of actors.
    val nodesArr = ArrayBuffer.empty[ActorRef]
    val rand = new Random(System.currentTimeMillis())

    // create array of all nodes (actors)    
    for (i <- 0 to noOfNodes - 1) {
      var node = actorOf(Props(new GossipWorker(2, i)), name = "Worker" + i)
      //node ! GossipWorker.Init(algorithm, topology)
      nodesArr += node
    }

    // end of constructor

    // Receive block for the Watcher.
    final def receive = {
      // send message to the first node to initiate after setting start time.
      case Initiate =>
        startTime = System.currentTimeMillis()

      // When Actors send Terminate Message to Watcher to remove from network.
      case Terminate(ref, sw) =>
        nodesArr -= ref
        // when all actors are down, shutdown the system.
        if (nodesArr.isEmpty) {
          val finalTime = System.currentTimeMillis()
          println("Final:" + (finalTime - startTime))
          context.system.shutdown
        }

      case _ => println("FAILED HERE")
    }
  }

  object GossipWorker {
    case class obj(nodeId: Int, ip: Int)
    case class Init(algo: String, topo: String)
  }

  class GossipWorker(b: Int, number: Int) extends Actor {
    import context._
    import GossipWorker._

    var watcherRef: ActorRef = null
    val rand = new Random(System.currentTimeMillis())
    var isAlive = true
    var cancellable: Cancellable = null
    var count = 0

    var base = math.pow(2, b).toInt
    var numberOfRows = math.ceil(128 / b).toInt

    var nodeId = MessageDigest.getInstance("MD5").digest(number.toString().getBytes).foldLeft("")((s: String, by: Byte) => s + convertDecimaltoBase(by & 0xFF, base))
    var leafArr = new Array[obj](base)
    var neighborArr = new Array[obj](base)
    var routingArr = Array.ofDim[obj](numberOfRows, base)

    // initialize routing array with greyed out cell for self
    var tmp = nodeId.toCharArray()
    while (count < tmp.length()) {
      val digit = tmp(count) - '0'
      routingArr(count)(digit) = new obj(-1, number)
      count += 1
    }
    count = 0

    def convertDecimaltoBase(no: Int, base: Int): String = {
      var tmp = no
      var str = ""
      while (tmp >= base) {
        str += tmp % base
        tmp = tmp / base
      }
      str += tmp
      while (str.length < 8 / (math.log(base) / math.log(2))) {
        str += 0
      }
      str.reverse
    }

    def updateLeafSet(arr: Array[obj]) {

    }

    def updateNeighborSet(arr: Array[obj]) {

    }

    def updateRoutingSet(arr: Array[Array[obj]]) {

    }

    // Receive Block for a normal Gossip Message
    def gossipNetwork: Receive = {
      case _ => println("FAILED")
    }

    // Receive Block for a normal Gossip Message
    def gossipFullNetwork: Receive = {
      case _ => println("FAILED")
    }

    def receive = LoggingReceive {
      case Init(algorithm, topology) =>
        watcherRef = sender
        if (topology == "full") {
          become(gossipFullNetwork)
        } else {
          become(gossipNetwork)
        }
      case _ => println("FAILED")
    }
  }
}