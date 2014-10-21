package project3.src;
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.DurationInt

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
    case class Terminate(node: Pastry.Node)
    case object Initiate
    case object SendLiveNeighbor
    case class AddNewNode(node: Pastry.Node)
    case class VerifyDestination(key: Pastry.Node, actual: Pastry.Node)
  }

  class Watcher(noOfNodes: Int, noOfRequests: Int) extends Actor {
    import Watcher._
    import context._
    var startTime = System.currentTimeMillis()
    var b = 2
    var mismatch = 0

    // keep track of actors.
    val nodesArr = ArrayBuffer.empty[Pastry.Node]

    // create array of all nodes (actors)    
    for (i <- 1 to noOfNodes) {
      var node = actorOf(Props(new Pastry(b)), name = "Worker" + i)
      system.scheduler.scheduleOnce(100 * i milliseconds, node, Pastry.Init)
    }
    // end of constructor

    // Receive block for the Watcher.
    final def receive = {
      // send message to the first node to initiate after setting start time.
      case Initiate =>
        startTime = System.currentTimeMillis()

      case SendLiveNeighbor =>
        var length = nodesArr.length
        var index = sender.path.name.drop(6).toInt
        if (length == 0) {
          sender ! Pastry.RecieveLiveNeighbor(null)
        } else {
          if (index >= length) {
            sender ! Pastry.RecieveLiveNeighbor(nodesArr(length - 1).nodeRef)
          } else {
            sender ! Pastry.RecieveLiveNeighbor(nodesArr(index - 1).nodeRef)
          }
        }

      case AddNewNode(node) =>
        nodesArr += node
        nodesArr.sortBy(_.nodeRef.path.name.drop(6).toInt)

      case VerifyDestination(key, actual) =>
        val tmp = nodesArr.minBy(a => (key.nodeId - a.nodeId).abs)
        if (tmp.nodeId != actual.nodeId) {
          println("ERROR - Key: " + key.nodeId + " Expected: " + tmp.nodeId + " Actual: " + actual.nodeId)
          mismatch += 1
        } else {
          println("Correct - Key: " + key.nodeId + " Expected: " + tmp.nodeId + " Actual: " + actual.nodeId)
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
    def forward(msg: String, key: Pastry.Node, node: Pastry.Node) {
      /* TODO Template */
    }

    def deliver(msg: String, key: Pastry.Node, hop: Int) {
      /* TODO Template */
    }

    def newLeafs(msg: Array[Pastry.Node]) {
      /* TODO Template */
    }
  }

  object Pastry {
    case class Node(var nodeId: Int, nodeRef: ActorRef)
    case class RouteMsg(msg: String, key: Node, hop: Int)
    case class FinalHopMsg(msg: String, key: Node, hop: Int)
    case class RecieveLiveNeighbor(nodeRef: ActorRef)
    case class RecieveStatus(arr: Array[Node], setType: String)
    case object Init
    case object FAILED
  }

  class Pastry(b: Int) extends Actor {
    import context._
    import Pastry._

    /* Constructor Started */
    var count = 0
    var selfNode = new Node(-1, self)
    var handler: Application = null

    var base = math.pow(2, b).toInt
    var noOfBits = 8 // since we take only first 8 digits of a hash.

    /* Constructor Ended */

    // declare state tables with default values as (0,null)
    var leafArr = Array.fill(base)(new Node(0, null))
    var neighborArr = Array.fill(base)(new Node(0, null))
    var routingArr = Array.fill(noOfBits)(Array.fill(base)(new Node(0, null))) // Get Node Id.

    // called by application to bring up Pastry node and add to the network.
    def pastryInit(handler: Application): Node = {
      this.handler = handler
      // take a crypto-hash and convert it to base 2^b. Then take first 8 bits of it.
      selfNode.nodeId = MessageDigest.getInstance("MD5").digest(self.path.name.drop(6).getBytes).foldLeft("")((s: String, by: Byte) => s + convertDecimaltoBase(by & 0xFF, base)).substring(0, noOfBits - 1).toInt

      // initialize routing array with current NodeId.
      var tmp = selfNode.nodeId.toString
      count = 0
      while (count < tmp.length()) {
        val digit = tmp(count) - '0'
        routingArr(count)(digit) = new Node(-1, self) // set matching columns with a -1 node object to indicate self.
        count += 1
      }

      // get Neighbor by Proximity. Ideal Solution is to use an increasing ring of multi-cast, but we will just use Watcher to query the same.
      parent ! Watcher.SendLiveNeighbor

      // when u get the neighbor, ask the guy to send a join msg, with your nodeId.
      return selfNode
    }

    // route message to node with closest key value.
    def route(msg: String, key: Node, hopCount: Int = 0): Boolean = {
      var currPrefixSize = 0
      val currNodeIdDiff = (key.nodeId - selfNode.nodeId).abs
      var strKey = key.nodeId.toString
      var tmpArr = leafArr.filter(a => a.nodeId > 0) // filter empty cells
      var found = false
      // if found in leaf set.
      if (tmpArr.length > 0 && key.nodeId >= tmpArr.minBy(a => a.nodeId).nodeId && key.nodeId <= tmpArr.maxBy(a => a.nodeId).nodeId) {

        val tmp = tmpArr.minBy(a => (key.nodeId - a.nodeId).abs)
        println("Routing " + strKey + " from Leaf Node to " + tmp.nodeId)
        // call to application.
        handler.forward(msg, key, tmp)
        tmp.nodeRef ! FinalHopMsg(msg, key, hopCount) // current assumption is that final node will always be routed from the leaf set.
        found = true

      } // search in routing table.
      else {

        // if appropriate entry found in routing Table, forward it.
        currPrefixSize = shl(strKey, selfNode.nodeId.toString)
        var routingEntry = routingArr(currPrefixSize)(strKey(currPrefixSize) - '0')
        if (routingEntry.nodeId != 0) {
          println("Routing " + strKey + " from Routing Table " + routingEntry.nodeId)
          // call to application.
          handler.forward(msg, key, routingEntry)
          routingEntry.nodeRef ! RouteMsg(msg, key, hopCount)
          found = true

        } // else, search all the data sets.        
        else {
          count = 0
          // Union all state tables
          tmpArr = leafArr
          tmpArr ++= neighborArr
          for (count <- 0 to routingArr.length - 1) {
            tmpArr ++= routingArr(count)
          }
          tmpArr = tmpArr.filter(a => a.nodeId > 0)

          while (count < tmpArr.length && !found) {
            var prefixSize = shl(strKey, tmpArr(count).nodeId.toString)
            if (prefixSize >= currPrefixSize) {
              var nodeDiff = (key.nodeId - tmpArr(count).nodeId).abs
              // if found probable node in the union.
              if (nodeDiff < currNodeIdDiff) {
                found = true
                handler.forward(msg, key, tmpArr(count)) // call to application.
                tmpArr(count).nodeRef ! RouteMsg(msg, key, hopCount)
                println("Routing " + strKey + " from Rare " + tmpArr(count).nodeId)
              }
            }
            count += 1
          } // end of while
        } // end of else
      } // end of else
      return found
    } // end of method

    def updateLeafSet(arr: Array[Node]) {
      var l = base / 2
      var lr = leafArr.splitAt(l)
      var count = 0
      while (count < arr.length) {
        var id = arr(count).nodeId
        // null implies empty cell
        if (id > 0) {
          if (id < selfNode.nodeId) {
            if (id > lr._1(0).nodeId) {
              lr._1(0) = arr(count)
              lr._1.sortBy(a => a.nodeId)
            }
          } else if (id > selfNode.nodeId) {
            if (lr._2(0).nodeId == 0) {
              lr._2(0) = arr(count)
              lr._2.sortBy(a => a.nodeId)
            } else if (id < lr._2.last.nodeId) {
              lr._2(l - 1) = arr(count)
              lr._2.sortBy(a => a.nodeId)
            }
          }
        }
        count += 1
      }
      leafArr = lr._1
      leafArr ++= lr._2
    }

    def updateNeighborSet(arr: Array[Node]) {
      var count = 0
      while (count < arr.length) {
        var id = arr(count).nodeId
        // 0 implies empty cell
        if (id > 0) {
          if (neighborArr(0).nodeId == 0) {
            neighborArr(0) = arr(count)
          } else if ((selfNode.nodeId - id).abs < (selfNode.nodeId - neighborArr(0).nodeId).abs) {
            neighborArr(0) = arr(count)
          }
          neighborArr.sortBy(a => a.nodeId)
        }
        count += 1
      }
    }

    def updateRoutingSet(arr: Array[Node]) {
      var ctr = 0
      while (ctr < arr.length) {
        var itemId = arr(ctr).nodeId
        if (itemId > 0) {
          var PrefixSize = shl(itemId.toString, selfNode.nodeId.toString)
          var routingEntry = routingArr(PrefixSize)(itemId.toString()(PrefixSize))

          if (routingEntry.nodeId > 0) {
            if ((selfNode.nodeId - itemId).abs < (selfNode.nodeId - routingEntry.nodeId).abs) {
              routingArr(PrefixSize)(itemId.toString()(PrefixSize)) = arr(ctr)
            }
          }
        }
        ctr += 1
      }
    }

    def sendStatus(key: Node, hop: Int) {
      if (hop == 0) {
        key.nodeRef ! RecieveStatus(neighborArr, "neighbor")
      }
      var PrefixSize = shl(key.nodeId.toString, selfNode.nodeId.toString)
      key.nodeRef ! RecieveStatus(routingArr(PrefixSize), "routing")
    }

    // sent by the newly added node to all the nodes in its tables.
    def sendStatusAfterJoin() {
      var ctr = 0
      while (ctr < leafArr.length) {
        if (leafArr(ctr).nodeId > 0) {
          leafArr(ctr).nodeRef ! RecieveStatus(leafArr ++ Array(selfNode), "leaf")
        }
        ctr += 1
      }
      ctr = 0
      while (ctr < neighborArr.length) {
        if (neighborArr(ctr).nodeId > 0) {
          neighborArr(ctr).nodeRef ! RecieveStatus(neighborArr ++ Array(selfNode), "neighbor")
        }
        ctr += 1
      }
      ctr = 0
      while (ctr < routingArr.length) {
        var col = 0
        while (col < routingArr(ctr).length) {
          if (routingArr(ctr)(col).nodeId > 0) {
            routingArr(ctr)(col).nodeRef ! RecieveStatus(routingArr(ctr) ++ Array(selfNode), "neighbor")
          }
          col += 1
        }
        ctr += 1
      }
    }

    // converts to base.
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

    // returns length of max prefix.
    def shl(key: String, nodeId: String): Int = {
      var count = 0
      while (count < noOfBits - 1 && (key(count) == nodeId(count))) {
        count += 1
      }
      return count
    }

    // Receive block when in Initializing State before Node is Alive.
    def Initializing: Receive = LoggingReceive {
      case Init =>
        pastryInit(new Application(self.path.name.drop(6).toInt))

      case RecieveLiveNeighbor(ref) =>
        if (ref != null) {
          ref ! RouteMsg("join", selfNode, -1)
        } else {
          // this is the first node.
          println("recd")
          parent ! Watcher.AddNewNode(selfNode)
          become(Alive)
        }

      case RecieveStatus(arr, setType) =>
        if (setType == "neighbor") {
          updateNeighborSet(arr)
        } else if (setType == "leaf") {
          updateLeafSet(arr)

          // leaf node is received only join has reached final destination.
          sendStatusAfterJoin()
          parent ! Watcher.AddNewNode(selfNode)
          become(Alive)

        } else {
          updateRoutingSet(arr)
        }

      case _ => println("FAILED")

    }

    // Receive block when in Alive State.
    def Alive: Receive = {
      case RouteMsg(msg, key, hop) =>
        var hopCount = hop + 1
        var forwarded = route(msg, key, hopCount)
        // send appropriate routing table entries and leaf table
        if (msg == "join") {
          sendStatus(key, hopCount)
          if (!forwarded) {
            handler.deliver(msg, key, hopCount)
            println("delivered " + key.nodeId + " to " + selfNode.nodeId + " with hop count " + hopCount)
            parent ! Watcher.VerifyDestination(key, selfNode)
            key.nodeRef ! RecieveStatus(leafArr ++ Array(selfNode), "leaf")
          }
        }

      case FinalHopMsg(msg, key, hop) =>
        var hopCount = hop + 1
        handler.deliver(msg, key, hopCount)
        println("delivered " + key.nodeId + " to " + selfNode.nodeId + " with hop count " + hopCount)
        // send appropriate routing table entries and leaf table
        if (msg == "join") {
          sendStatus(key, hopCount)
          parent ! Watcher.VerifyDestination(key, selfNode)
          key.nodeRef ! RecieveStatus(leafArr ++ Array(selfNode), "leaf")
        }

      case RecieveStatus(arr, setType) =>
        if (setType == "neighbor") {
          updateNeighborSet(arr)
        } else if (setType == "leaf") {
          updateLeafSet(arr)
        } else {
          updateRoutingSet(arr)
        }
    }

    // default state of Actor.
    def receive = Initializing
  }
}