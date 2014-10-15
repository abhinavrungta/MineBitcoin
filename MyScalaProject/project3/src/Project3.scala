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
    var numberOfRows = 8 // since we take only first 8 digits of a hash.

    // Get Node Id.
    var currentNodeId = MessageDigest.getInstance("MD5").digest(number.toString().getBytes).foldLeft("")((s: String, by: Byte) => s + convertDecimaltoBase(by & 0xFF, base)).substring(0, 7).toInt

    // declare state tables.
    var leftLeafArr = new Array[obj](base / 2)
    var rightLeafArr = new Array[obj](base / 2)
    var neighborArr = new Array[obj](base)
    var routingArr = Array.ofDim[obj](numberOfRows, base)

    // initialize routing array with current NodeId.
    var tmp = currentNodeId.toString
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
      return str
    }

    def updateLeafSet(arr: Array[obj]) {

    }

    def updateNeighborSet(arr: Array[obj]) {

    }

    def updateRoutingSet(arr: Array[Array[obj]]) {

    }

    def route(msg: String, key: Int) {
      var currPrefixSize = 0
      val currNodeIdDiff = (key - currentNodeId).abs
      var strKey = key.toString

      // if found in leaf set.
      if (key >= leftLeafArr.minBy(a => a.nodeId).nodeId && key <= rightLeafArr.maxBy(a => a.nodeId).nodeId) {
        val tmp = leftLeafArr.minBy(a => (key - a.nodeId).abs)
        val tmp2 = rightLeafArr.minBy(a => (key - a.nodeId).abs)
        if (tmp.nodeId < tmp2.nodeId) {
          forward(msg, tmp.nodeId)
        } else {
          forward(msg, tmp2.nodeId)
        }
      } else {
        // search in routing table.
        currPrefixSize = shl(strKey, currentNodeId.toString)
        var routingEntry = routingArr(currPrefixSize)(strKey(currPrefixSize))
        // if appropriate entry found, forward it.
        if (routingEntry != null) {
          forward(msg, routingEntry.nodeId)
        } else {
          // else, search all the data sets.
          var tmpArr = leftLeafArr
          tmpArr ++= rightLeafArr
          tmpArr ++= neighborArr
          for (count <- currPrefixSize to routingArr.length - 1) {
            tmpArr ++= routingArr(count)
          }

          count = 0
          var found = false
          while (count < tmpArr.length && !found) {
            if (tmpArr(count).nodeId == -1) {
              // if it is the current node id, ignore.
              count += 1
            } else {
              var prefixSize = shl(strKey, tmpArr(count).nodeId.toString)
              if (prefixSize >= currPrefixSize) {
                var nodeDiff = (key - tmpArr(count).nodeId).abs
                if (nodeDiff < currNodeIdDiff) {
                  found = true
                  forward(msg, tmpArr(count).nodeId)
                }
              }
            }
            // end of while
          }
          // end of else
        }
        // end of else
      }
      // end of method
    }

    def forward(msg: String, key: Int) {
    }

    def shl(key: String, nodeId: String): Int = {
      var count = 0
      while (count < 8 - 1 && (key(count) == nodeId(count))) {
        count += 1
      }
      return count
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