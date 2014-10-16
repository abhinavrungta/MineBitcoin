package project3.src;
import scala.collection.mutable.ArrayBuffer

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
    case class Terminate(ref: ActorRef)
    case object Initiate
    case object getLiveNeighbor
  }

  class Watcher(noOfNodes: Int, noOfRequests: Int) extends Actor {
    import Watcher._
    import context._
    var startTime = System.currentTimeMillis()
    var b = 2

    // keep track of actors.
    val nodesArr = ArrayBuffer.empty[ActorRef]

    // create array of all nodes (actors)    
    for (i <- 0 to noOfNodes - 1) {
      var node = actorOf(Props(new Pastry(b)), name = "Worker" + i)
      //node ! GossipWorker.Init(algorithm, topology)
      nodesArr += node
    }
    // end of constructor

    // Receive block for the Watcher.
    final def receive = {
      // send message to the first node to initiate after setting start time.
      case Initiate =>
        startTime = System.currentTimeMillis()

      case Watcher.getLiveNeighbor =>
        var index = nodesArr.indexOf(sender)
        var length = nodesArr.length
        if (index != -1) {
          if (length != 1) {
            if (index == 0) {
              sender ! Pastry.Neighbor(nodesArr(index + 1))
            } else {
              sender ! Pastry.Neighbor(nodesArr(index - 1))
            }
          }
        }

      // When Actors send Terminate Message to Watcher to remove from network.
      case Terminate(ref) =>
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

  class Application(nodeNumber: Int) {
    def forward(msg: String, key: Int, node: Pastry.node) {
      /* TODO Template */
    }

    def deliver(msg: String, key: Int) {
      /* TODO Template */
      println("delivered")
    }

    def newLeafs(msg: Array[Pastry.node]) {
      /* TODO Template */
    }
  }

  object Pastry {
    case class node(nodeId: Int, nodeRef: ActorRef)
    case class routeMsg(msg: String, key: Int)
    case class Neighbor(nodeRef: ActorRef)
    case object Init
  }

  class Pastry(b: Int) extends Actor {
    import context._
    import Pastry._

    var watcherRef: ActorRef = null
    var isAlive = false
    var cancellable: Cancellable = null
    var count = 0
    var selfNodeId = -1
    var handler: Application = null

    var base = math.pow(2, b).toInt
    var numberOfRows = 8 // since we take only first 8 digits of a hash.

    // declare state tables.
    var leftLeafArr = new Array[node](base / 2)
    var rightLeafArr = new Array[node](base / 2)
    var neighborArr = new Array[node](base)
    var routingArr = Array.ofDim[node](numberOfRows, base) // Get Node Id.

    // called by application to bring up Pastry node and add to the network.
    def pastryInit(handler: Application): Int = {
      this.handler = handler
      var nodeNumber = self.path.name.drop(6).toInt
      selfNodeId = MessageDigest.getInstance("MD5").digest(nodeNumber.toString().getBytes).foldLeft("")((s: String, by: Byte) => s + convertDecimaltoBase(by & 0xFF, base)).substring(0, 8 - 1).toInt

      // initialize routing array with current NodeId.
      var tmp = selfNodeId.toString
      while (count < tmp.length()) {
        val digit = tmp(count) - '0'
        routingArr(count)(digit) = new node(-1, self)
        count += 1
      }

      // get Neighbor by Proximity. Ideal Solution is to use an increasing ring of multi-cast, but we will just use Watcher to query the same.
      parent ! Watcher.getLiveNeighbor

      // when u get the neighbor, ask the guy to send a join msg, with your nodeId.
      return selfNodeId
    }

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

    def updateLeafSet(arr: Array[node]) {

    }

    def updateNeighborSet(arr: Array[node]) {

    }

    def updateRoutingSet(arr: Array[Array[node]]) {

    }

    // route message to node with closest key value.
    def route(msg: String, key: Int) {
      var currPrefixSize = 0
      val currNodeIdDiff = (key - selfNodeId).abs
      var strKey = key.toString

      // if found in leaf set.
      if (key >= leftLeafArr.minBy(a => a.nodeId).nodeId && key <= rightLeafArr.maxBy(a => a.nodeId).nodeId) {
        val tmp = leftLeafArr.minBy(a => (key - a.nodeId).abs)
        val tmp2 = rightLeafArr.minBy(a => (key - a.nodeId).abs)
        if (tmp.nodeId < tmp2.nodeId) {
          // call to application.
          handler.forward(msg, key, tmp)
          tmp.nodeRef ! routeMsg(msg, key)
        } else {
          // call to application.
          handler.forward(msg, key, tmp2)
          tmp2.nodeRef ! routeMsg(msg, key)
        }
      } else {
        // search in routing table.
        currPrefixSize = shl(strKey, selfNodeId.toString)
        var routingEntry = routingArr(currPrefixSize)(strKey(currPrefixSize))
        // if appropriate entry found, forward it.
        if (routingEntry != null) {
          // call to application.
          handler.forward(msg, key, routingEntry)
          routingEntry.nodeRef ! routeMsg(msg, key)
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
                  // call to application.
                  handler.forward(msg, key, tmpArr(count))
                  tmpArr(count).nodeRef ! routeMsg(msg, key)
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

    def shl(key: String, nodeId: String): Int = {
      var count = 0
      while (count < 8 - 1 && (key(count) == nodeId(count))) {
        count += 1
      }
      return count
    }

    def receive = LoggingReceive {
      case Init =>
        watcherRef = sender
        pastryInit(new Application(self.path.name.drop(6).toInt))

      case Neighbor(ref) =>
        ref ! routeMsg("join", selfNodeId)

      case routeMsg(msg, key) =>
        route(msg, key)

      case _ => println("FAILED")
    }
  }
}