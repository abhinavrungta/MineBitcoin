package project3.src;

import java.security.MessageDigest

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.DurationInt

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.actorRef2Scala
import akka.event.LoggingReceive

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
    var b = 2
    var mismatch = 0

    // keep track of actors.
    var nodesArr = ArrayBuffer.empty[Pastry.Node]

    // create array of all nodes (actors)    
    for (i <- 1 to noOfNodes) {
      var node = actorOf(Props(new Pastry(b)), name = "Worker" + i)
      system.scheduler.scheduleOnce(20 * i milliseconds, node, Pastry.Init)
    }
    // end of constructor

    // Receive block for the Watcher.
    final def receive = {

      case SendLiveNeighbor =>
        // if it is the first node, nodesArr will be empty.
        if (nodesArr.length == 0) {
          sender ! Pastry.LiveNeighbor(null)
        } else {
          // find the closest based on proximity metric. (in this case the actor number)
          var closestNeighbor = nodesArr.minBy(a => (a.nodeRef.path.name.drop(6).toInt - sender.path.name.drop(6).toInt).abs)
          sender ! Pastry.LiveNeighbor(closestNeighbor.nodeRef)
        }

      case AddNewNode(node) =>
        nodesArr += node
        println("Added node " + node.nodeRef.path.name.drop(6) + " with nodeId " + node.nodeId)
        // For Debugging Ask all the guys to print the pastry tables.
        var ctr1 = 0
        while (ctr1 < nodesArr.length) {
          nodesArr(ctr1).nodeRef ! Pastry.PrintTable
          ctr1 += 1
        }

      case VerifyDestination(key, actual) =>
        val expectedNode = nodesArr.minBy(a => (key.nodeId - a.nodeId).abs)
        if (expectedNode.nodeId != actual.nodeId) {
          println("ERROR - Key: " + key.nodeId + " Expected: " + expectedNode.nodeId + " Actual: " + actual.nodeId)
          mismatch += 1
        } else {
          println("Correct - Key: " + key.nodeId + " Expected: " + expectedNode.nodeId + " Actual: " + actual.nodeId)
        }

      // When Actors send Terminate Message to Watcher to remove from network.
      case Terminate(ref) =>
        nodesArr -= ref
        // when all actors are down, shutdown the system.
        if (nodesArr.isEmpty) {
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
    case class LiveNeighbor(nodeRef: ActorRef)
    case class UpdateTable(arr: Array[Node], setType: String)
    case object Init
    case object FAILED
    case object PrintTable
  }

  class Pastry(b: Int) extends Actor {
    import context._
    import Pastry._

    /* Constructor Started */
    val selfProxyId = self.path.name.drop(6).toInt
    var selfNode = new Node(-1, self)
    var handler: Application = null

    var base = math.pow(2, b).toInt
    var noOfBits = 8 // since we take only first 8 digits of a hash.

    // declare state tables with default values as null
    var leafArr = new Array[Node](0)
    var neighborArr = new Array[Node](0)
    var routingArr = Array.ofDim[Node](noOfBits, base)

    /* Constructor Ended */

    // called by application to bring up Pastry node and add to the network.
    def pastryInit(handler: Application): Node = {
      this.handler = handler
      // take a crypto-hash and convert it to base 2^b. Then take first 8 bits of it.
      selfNode.nodeId = MessageDigest.getInstance("MD5").digest(selfProxyId.toString.getBytes).foldLeft("")((s: String, by: Byte) => s + convertDecimaltoBase(by & 0xFF, base)).substring(0, noOfBits).toInt

      // initialize routing array with current NodeId.
      var tmp = getString(selfNode.nodeId)
      var ctr2 = 0
      while (ctr2 < tmp.length()) {
        val digit = tmp(ctr2) - '0'
        routingArr(ctr2)(digit) = new Node(-1, self) // set matching columns with a -1 node object to indicate self.
        ctr2 += 1
      }

      // get Neighbor by Proximity. Ideal Solution is to use an increasing ring of multi-cast, but we will just use Watcher to query the same.
      parent ! Watcher.SendLiveNeighbor

      // when u get the neighbor, ask the guy to send a join msg, with your nodeId.
      return selfNode
    }

    // route message to node with closest key value.
    def route(msg: String, key: Node, hopCount: Int = 0): Boolean = {
      var found = false
      val currNodeIdDiff = (key.nodeId - selfNode.nodeId).abs

      var tmpArr = leafArr.sortBy(a => a.nodeId)
      // if found in leaf set.
      if (tmpArr.length > 0 && key.nodeId >= tmpArr.head.nodeId && key.nodeId <= tmpArr.last.nodeId) {

        val tmp = tmpArr.minBy(a => (key.nodeId - a.nodeId).abs)
        // chk if current node is not the least.
        if ((key.nodeId - tmp.nodeId).abs < currNodeIdDiff) {
          println("Self: " + selfNode.nodeId + " Routing " + key.nodeId + " to Leaf Node " + tmp.nodeId)
          // call to application.
          handler.forward(msg, key, tmp)
          tmp.nodeRef ! FinalHopMsg(msg, key, hopCount) // current assumption is that final node will always be routed from the leaf set.
          found = true
        }
      } // search in routing table.
      else {

        // if appropriate entry found in routing Table, forward it.
        var currPrefixSize = shl(getString(key.nodeId), getString(selfNode.nodeId))
        var routingEntry = routingArr(currPrefixSize)(getString(key.nodeId)(currPrefixSize) - '0')
        if (routingEntry != null) {
          println("Self: " + selfNode.nodeId + " Routing " + key.nodeId + " to Routing Table " + routingEntry.nodeId)
          // call to application.
          handler.forward(msg, key, routingEntry)
          routingEntry.nodeRef ! RouteMsg(msg, key, hopCount)
          found = true

        } // else, search all the data sets.        
        else {
          var ctr3 = 0
          // Union all state tables
          tmpArr = leafArr
          tmpArr ++= neighborArr
          for (ctr3 <- 0 to routingArr.length - 1) {
            tmpArr ++= routingArr(ctr3)
          }
          tmpArr = tmpArr.filter(a => a != null && a.nodeId != -1)

          ctr3 = 0
          var minimumNodeIdDiff = currNodeIdDiff
          var closestNode = selfNode
          while (ctr3 < tmpArr.length) {
            var prefixSize = shl(getString(key.nodeId), getString(tmpArr(ctr3).nodeId))
            if (prefixSize >= currPrefixSize) {
              var nodeDiff = (key.nodeId - tmpArr(ctr3).nodeId).abs
              // if found probable node in the union.
              if (nodeDiff < minimumNodeIdDiff) {
                closestNode = tmpArr(ctr3)
                minimumNodeIdDiff = nodeDiff
                found = true
              }
            }
            ctr3 += 1
          } // end of while
          if (found) {
            handler.forward(msg, key, closestNode) // call to application.
            closestNode.nodeRef ! RouteMsg(msg, key, hopCount)
            println("Self: " + selfNode.nodeId + " Routing " + key.nodeId + " to Rare " + closestNode.nodeId)
          }
        } // end of else
      } // end of else
      return found
    } // end of method

    def updateLeafSet(arr: Array[Node]) {
      val size = base / 2
      var l = leafArr.filter(a => a.nodeId < selfNode.nodeId)
      var r = leafArr.filter(a => a.nodeId > selfNode.nodeId)

      var count = 0
      var tmpArr = arr.distinct
      while (count < tmpArr.length) {
        var item = tmpArr(count)
        // if item < selfNodeId, it possibly goes in left array.
        if (item.nodeId < selfNode.nodeId) {
          l = updateLeafWithItemIfValid(item, l, size)
        } else if (item.nodeId > selfNode.nodeId) {
          r = updateLeafWithItemIfValid(item, r, size)
        }
        count += 1
      }
      leafArr = l
      leafArr ++= r
    }

    def updateNeighborSet(arr: Array[Node]) {
      var count = 0
      var tmpArr = arr.distinct

      while (count < tmpArr.length) {
        var item = tmpArr(count)
        // if it is the same as current node, ignore, else proceed
        if (item.nodeId != selfNode.nodeId) {
          neighborArr = updateNeighborWithItemIfValid(item, neighborArr, base)
        }
        count += 1
      }
    }

    def updateRoutingSet(arr: Array[Node]) {
      var ctr = 0
      var tmpArr = arr.distinct
      while (ctr < tmpArr.length) {
        var item = tmpArr(ctr)
        var itemIdStr = getString(item.nodeId)

        if (item.nodeId != selfNode.nodeId) {
          var PrefixSize = shl(itemIdStr, getString(selfNode.nodeId))
          var routingEntry = routingArr(PrefixSize)(itemIdStr(PrefixSize) - '0')

          if (routingEntry != null) {
            if ((selfProxyId - item.nodeRef.path.name.drop(6).toInt).abs < (selfProxyId - routingEntry.nodeRef.path.name.drop(6).toInt).abs) {
              routingArr(PrefixSize)(itemIdStr(PrefixSize) - '0') = item
            }
          } else {
            routingArr(PrefixSize)(itemIdStr(PrefixSize) - '0') = item
          }
        }
        ctr += 1
      }
    }

    def sendStatus(key: Node, hop: Int) {
      if (hop == 0) {
        var tmpArr = neighborArr.filter(a => a != null && a.nodeId != -1)
        key.nodeRef ! UpdateTable(tmpArr ++ Array(selfNode), "neighbor")
      }
      var PrefixSize = shl(getString(key.nodeId), getString(selfNode.nodeId))
      var tmpArr = routingArr(PrefixSize).filter(a => a != null && a.nodeId != -1)
      key.nodeRef ! UpdateTable(tmpArr ++ Array(selfNode), "routing")
    }

    // sent by the newly added node to all the nodes in its tables.
    def sendStatusAfterJoin() {
      var ctr = 0
      var tmpArr = leafArr
      tmpArr ++= neighborArr
      for (ctr <- 0 to routingArr.length - 1) {
        tmpArr ++= routingArr(ctr)
      }
      tmpArr = tmpArr.filter(a => a != null && a.nodeId != -1)

      ctr = 0
      while (ctr < tmpArr.length) {
        tmpArr(ctr).nodeRef ! UpdateTable(tmpArr ++ Array(selfNode), "all")
        ctr += 1
      }
    }

    def updateLeafWithItemIfValid(item: Node, arr: Array[Node], size: Int): Array[Node] = {
      var l = arr
      // proceed only if element is not already present.
      if (l.indexOf(item) == -1) {
        // when array is not full, add without checking.
        if (l.size < size) {
          l = l ++ Array(item)
        } // else check if it can replace somebody.
        else {
          var maxItem = l.maxBy(a => (selfNode.nodeId - a.nodeId).abs)
          var index = l.indexOf(maxItem)
          if ((selfNode.nodeId - item.nodeId).abs < (selfNode.nodeId - maxItem.nodeId).abs) {
            l(index) = item
          }
        }
      }
      return l
    }

    def updateNeighborWithItemIfValid(item: Node, arr: Array[Node], size: Int): Array[Node] = {
      var l = arr
      // proceed only if element is not already present.
      if (l.indexOf(item) == -1) {
        // when array is not full, add without checking.
        if (l.size < size) {
          l = l ++ Array(item)
        } // else check if it can replace somebody.
        else {
          var maxItem = l.maxBy(a => (selfProxyId - a.nodeRef.path.name.drop(6).toInt).abs)
          var index = l.indexOf(maxItem)
          if ((selfProxyId - item.nodeRef.path.name.drop(6).toInt).abs < (selfProxyId - maxItem.nodeRef.path.name.drop(6).toInt).abs) {
            l(index) = item
          }
        }
      }
      return l
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

    // add leading zeros to the string
    def getString(nodeId: Int): String = {
      var id = nodeId.toString
      var count = noOfBits - id.length()
      var prefix = ""
      while (count > 0) {
        prefix += "0"
        count -= 1
      }
      prefix += id
      return prefix
    }

    def print() {
      var ctr = 0
      var str = "(leaf) Id: " + selfProxyId + "::"
      while (ctr < leafArr.length) {
        str += "\t" + leafArr(ctr).nodeId
        ctr += 1
      }
      println(str)

      ctr = 0
      str = "(neighbor) Id: " + selfProxyId + "::"
      while (ctr < neighborArr.length) {
        str += "\t" + neighborArr(ctr).nodeId
        ctr += 1
      }
      println(str)

      ctr = 0
      str = "(routing) Id: " + selfProxyId + "::"
      while (ctr < routingArr.length) {
        var col = 0
        while (col < routingArr(ctr).length) {
          if (routingArr(ctr)(col) != null && routingArr(ctr)(col).nodeId != -1) {
            str += "\t" + routingArr(ctr)(col).nodeId
          }
          col += 1
        }
        ctr += 1
      }
      println(str)
    }

    // Receive block when in Initializing State before Node is Alive.
    def Initializing: Receive = LoggingReceive {
      case Init =>
        println("******************************************************************")
        pastryInit(new Application(self.path.name.drop(6).toInt))

      case LiveNeighbor(ref) =>
        if (ref != null) {
          ref ! RouteMsg("join", selfNode, -1)
        } else {
          // this is the first node.
          println("recd")
          parent ! Watcher.AddNewNode(selfNode)
          become(Alive)
        }

      case UpdateTable(arr, setType) =>
        if (setType == "neighbor") {
          updateNeighborSet(arr)
        } else if (setType == "leaf") {
          updateLeafSet(arr)
          // leaf node is received only join has reached final destination.
          sendStatusAfterJoin()
          parent ! Watcher.AddNewNode(selfNode)
          become(Alive)

        } else if (setType == "routing") {
          updateRoutingSet(arr)
        } else if (setType == "all") {
          updateNeighborSet(arr)
          updateLeafSet(arr)
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
            //println("delivered " + key.nodeId + " to " + selfNode.nodeId + " with hop count " + hopCount)
            parent ! Watcher.VerifyDestination(key, selfNode)
            key.nodeRef ! UpdateTable(leafArr ++ Array(selfNode), "leaf")
          }
        }

      case FinalHopMsg(msg, key, hop) =>
        var hopCount = hop + 1
        handler.deliver(msg, key, hopCount)
        //println("delivered " + key.nodeId + " to " + selfNode.nodeId + " with hop count " + hopCount)
        // send appropriate routing table entries and leaf table
        if (msg == "join") {
          sendStatus(key, hopCount)
          parent ! Watcher.VerifyDestination(key, selfNode)
          key.nodeRef ! UpdateTable(leafArr ++ Array(selfNode), "leaf")
        }

      case UpdateTable(arr, setType) =>
        if (setType == "neighbor") {
          updateNeighborSet(arr)
        } else if (setType == "leaf") {
          updateLeafSet(arr)
        } else if (setType == "routing") {
          updateRoutingSet(arr)
        } else if (setType == "all") {
          updateNeighborSet(arr)
          updateLeafSet(arr)
          updateRoutingSet(arr)
        }

      case PrintTable =>
        print()
    }

    // default state of Actor.
    def receive = Initializing
  }
}