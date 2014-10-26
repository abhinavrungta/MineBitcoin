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
import akka.actor.Cancellable

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
      /* creates a watcher Actor. In the constructor, it starts joining nodes one by one to the n/w.
       * Once that is done, it starts sending messages. */
      val watcher = system.actorOf(Props(new Watcher(numNodes, numRequests)), name = "Watcher")
    }
  }

  object Watcher {
    case class Terminate(node: Pastry.Node)
    case object RouteMessages
    case object SendLiveNeighbor
    case class AddNewNode(node: Pastry.Node)
    case class VerifyDestination(key: Pastry.Node, actual: Pastry.Node, msg: String, hops: Int)
  }

  class Watcher(noOfNodes: Int, noOfRequests: Int) extends Actor {
    import Watcher._
    import context._
    var b = 3
    var base = math.pow(2, b).toInt
    var noOfBits = 8 // since we take only first 8 digits of a hash.
    var mismatch = 0
    var cancellable: Cancellable = null
    var count = 0
    // initialize totalHops Map for all the msg.
    var totalHops = scala.collection.mutable.Map[String, Int]()
    totalHops("join") = 0
    for (i <- 1 to noOfRequests) {
      totalHops("route" + i) = 0
    }
    // keep track of actors and application obj.
    var nodesArr = ArrayBuffer.empty[Pastry.Node]
    var applicationArr = ArrayBuffer.empty[Application]

    // create nodes (actors) and asks them to join to the n/w at intervals of 10 ms. It will then wait for all of them to join.
    // add first actor immediately and the rest after a second.
    var node = actorOf(Props(new Pastry(base, noOfBits)), name = "Worker1")
    var app = new Application(node)
    system.scheduler.scheduleOnce(0 milliseconds, node, Pastry.Init(app))
    applicationArr += app

    for (i <- 2 to noOfNodes) {
      node = actorOf(Props(new Pastry(base, noOfBits)), name = "Worker" + i)
      app = new Application(node)
      system.scheduler.scheduleOnce(1000 + 10 * i milliseconds, node, Pastry.Init(app))
      applicationArr += app
    }
    // end of constructor

    // Receive block for the Watcher.
    final def receive = LoggingReceive {

      case SendLiveNeighbor =>
        // if it is the first node, nodesArr will be empty.
        if (nodesArr.length == 0) {
          sender ! Pastry.LiveNeighbor(null)
        } else {
          // find the closest based on proximity metric. (in this case the actor number)
          var closestNeighbor = nodesArr.minBy(a => (a.nodeRef.path.name.drop(6).toInt - sender.path.name.drop(6).toInt).abs)
          sender ! Pastry.LiveNeighbor(closestNeighbor.nodeRef)
        }

      case VerifyDestination(key, actual, msg, hop) =>
        totalHops(msg) = totalHops(msg) + hop
        val expectedNode = nodesArr.minBy(a => (key.nodeId - a.nodeId).abs)
        if (expectedNode.nodeId != actual.nodeId) {
          println("ERROR - Key: " + key.nodeId + " Expected: " + expectedNode.nodeId + " Actual: " + actual.nodeId)
          mismatch += 1
        } else {
          println("Correct - Key: " + key.nodeId + " Expected: " + expectedNode.nodeId + " Actual: " + actual.nodeId)
        }

      case AddNewNode(node) =>
        nodesArr += node
        // when all the nodes have joined, start routing messages. Call at intervals of 1 sec.
        if (nodesArr.length == noOfNodes) {
          cancellable = system.scheduler.schedule(0 seconds, 1000 milliseconds, self, RouteMessages)
        }
      //println("Added node " + node.nodeRef.path.name.drop(6) + " with nodeId " + node.nodeId)
      // For Debugging Ask all the guys to print the pastry tables.
      //        var ctr1 = 0
      //        while (ctr1 < nodesArr.length) {
      //          nodesArr(ctr1).nodeRef ! Pastry.PrintTable
      //          ctr1 += 1
      //        }

      case RouteMessages =>
        count += 1
        if (count <= noOfRequests) {
          println("sending msg")
          applicationArr.foreach(a => a.pastryRef ! Pastry.RouteMsg("route" + count, new Pastry.Node(getRandomKey().toInt, Actor.noSender), -1))
        } else {
          cancellable.cancel
          println("mismatched routes " + mismatch)
          totalHops.foreach { keyVal => println(keyVal._1 + "=" + keyVal._2) }
          context.system.shutdown
        }

      case _ => println("FAILED HERE")
    }

    private def getRandomKey(): String = {
      val rnd = new scala.util.Random
      var str = ""
      (1 to noOfBits) foreach (x => str += rnd.nextInt(base))
      return str
    }
  }

  class Application(nodeRef: ActorRef) {
    var pastryRef: ActorRef = nodeRef

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
    case class LiveNeighbor(nodeRef: ActorRef)
    case class UpdateTable(arr: Array[Node], setType: String)
    case class Init(handler: Application)
    case object FAILED
    case object PrintTable
  }

  class Pastry(base: Int, noOfBits: Int) extends Actor {
    import context._
    import Pastry._

    /* Constructor Started */
    private val selfProxyId = self.path.name.drop(6).toInt
    private var selfNode = new Node(-1, self)
    private var handler: Application = null

    // declare state tables with default values as null
    private var leafArr = new Array[Node](0)
    private var neighborArr = new Array[Node](0)
    private var routingArr = Array.ofDim[Node](noOfBits, base)

    /* Constructor Ended */

    // public method called by application to bring up Pastry node and add to the network.
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

    // Public method to route message to node with closest key value.
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
          tmp.nodeRef ! RouteMsg(msg, key, hopCount)
          found = true
        }
      } // search in routing table if not found in leaf table.
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
          // forward to node with minimum node difference with key. 
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
          // if found, forward, else return false
          if (found) {
            handler.forward(msg, key, closestNode) // call to application.
            closestNode.nodeRef ! RouteMsg(msg, key, hopCount)
            println("Self: " + selfNode.nodeId + " Routing " + key.nodeId + " to Rare " + closestNode.nodeId)
          }
        } // end of else
      } // end of else
      return found
    } // end of method

    // update Leaf Set Table
    private def updateLeafSet(arr: Array[Node]) {
      // size param to limit size of left and right tables when updating.
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
      // reassign updated tables to leafArr.
      leafArr = l
      leafArr ++= r
    }

    // update Neighbor Set Table
    private def updateNeighborSet(arr: Array[Node]) {
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

    // update routing table
    private def updateRoutingSet(arr: Array[Node]) {
      var ctr = 0
      var tmpArr = arr.distinct
      while (ctr < tmpArr.length) {
        var item = tmpArr(ctr)
        var itemIdStr = getString(item.nodeId)

        // if it is not the current nodeId, proceed
        if (item.nodeId != selfNode.nodeId) {
          var PrefixSize = shl(itemIdStr, getString(selfNode.nodeId))
          var routingEntry = routingArr(PrefixSize)(itemIdStr(PrefixSize) - '0')

          // if destination is not empty, check if absolute difference of nodeId is less than current item's nodeId.
          if (routingEntry != null) {
            if ((selfProxyId - item.nodeRef.path.name.drop(6).toInt).abs < (selfProxyId - routingEntry.nodeRef.path.name.drop(6).toInt).abs) {
              routingArr(PrefixSize)(itemIdStr(PrefixSize) - '0') = item
            }
          } // else, update
          else {
            routingArr(PrefixSize)(itemIdStr(PrefixSize) - '0') = item
          }
        }
        ctr += 1
      }
    }

    // send Appropriate tables, in each hop.
    private def sendStatus(key: Node, hop: Int) {
      // if this is the first hop, also send the neighbor table.
      if (hop == 0) {
        var tmpArr = neighborArr.filter(a => a != null && a.nodeId != -1)
        key.nodeRef ! UpdateTable(tmpArr ++ Array(selfNode), "neighbor")
      }
      var PrefixSize = shl(getString(key.nodeId), getString(selfNode.nodeId))
      var tmpArr = routingArr(PrefixSize).filter(a => a != null && a.nodeId != -1)
      key.nodeRef ! UpdateTable(tmpArr ++ Array(selfNode), "routing")
    }

    // sent by the newly added node's tables to all the nodes in its tables.
    private def sendStatusAfterJoin() {
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

    // update leaf table with item if valid
    private def updateLeafWithItemIfValid(item: Node, arr: Array[Node], size: Int): Array[Node] = {
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

    // update neighbor table with item if valid
    private def updateNeighborWithItemIfValid(item: Node, arr: Array[Node], size: Int): Array[Node] = {
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
    private def convertDecimaltoBase(no: Int, base: Int): String = {
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
    private def shl(key: String, nodeId: String): Int = {
      var count = 0
      while (count < noOfBits - 1 && (key(count) == nodeId(count))) {
        count += 1
      }
      return count
    }

    // add leading zeros to the string
    private def getString(nodeId: Int): String = {
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

    // For debugging, print routing table of current node
    private def print() {
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
      case Init(applicationHandler) =>
        println("******************************************************************")
        pastryInit(applicationHandler)

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
          // leaf node is received only join has reached final destination. Send routing table to everyone, notify Watcher and become Alive.
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
    def Alive: Receive = LoggingReceive {
      case RouteMsg(msg, key, hop) =>
        var hopCount = hop + 1
        var forwarded = route(msg, key, hopCount)
        // if not forwarded, then this is the final destination.
        if (!forwarded) {
          handler.deliver(msg, key, hopCount)
          println("Delivered Msg Type " + msg + " with NodeId " + key.nodeId + " to Node # " + selfProxyId + " with NodeId " + selfNode.nodeId + " with hop count " + hopCount)
          parent ! Watcher.VerifyDestination(key, selfNode, msg, hopCount)
        }
        // if msg type is join, send appropriate routing table entries and leaf table.
        if (msg == "join") {
          sendStatus(key, hopCount)
          // if not forwarded, then this is final destination, also send the leaf table.
          if (!forwarded) {
            key.nodeRef ! UpdateTable(leafArr ++ Array(selfNode), "leaf")
          }
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