import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import com.typesafe.config.ConfigFactory

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.Terminated
import akka.event.LoggingReceive

object Project2 {
  val nodesArr = ArrayBuffer.empty[ActorRef]
  var noOfNodes = 10 // no of Actors to create on invoking the program.
  var topology = "" // Type of Topology
  var algorithm = "" // Type of Protocol
  val rand = new Random(System.currentTimeMillis())

  def main(args: Array[String]) {
    // exit if argument not passed as command line param
    if (args.length < 3) {
      println("INVALID NO OF ARGS.  USAGE :")
      println("1. Number of Nodes")
      println("2. Topology - full, 2D, line, imp2D")
      println("3. Protocol - push-sum, gossip")
      System.exit(1)
    } else if (args.length == 3) {
      noOfNodes = args(0).toInt
      topology = args(1)
      algorithm = args(2)

      // create actor system and a watcher actor
      val system = ActorSystem("Gossip")
      val watcher = system.actorOf(Props(new Watcher()), name = "Watcher")

      // convert no of nodes into Perfect Square if topology is 2D or imp2D
      if (topology == "2D" || topology == "imp2D") {
        var tmp = Math.sqrt(noOfNodes.toDouble).ceil
        tmp = tmp * tmp
        noOfNodes = tmp.toInt
      }

      // create array of all nodes (actors)
      for (i <- 0 to noOfNodes - 1) {
        var node = ActorRef.noSender
        // if algorithm type is push sum, use a certain type of object
        if (algorithm == "push-sum") {
          node = system.actorOf(Props(new PushSumWorker()), name = "Worker" + i)
        } else {
          node = system.actorOf(Props(new GossipWorker()), name = "Worker" + i)
        }
        nodesArr += node
        watcher ! Watcher.WatchMe(node)
      }

      // send list of neighbors to all nodes (actors) 
      for (i <- 0 to noOfNodes - 1) {
        var neighbours = ArrayBuffer.empty[ActorRef]
        if (topology == "full") {
          neighbours = getFullNetworkNodes(i)
        } else if (topology == "2D") {
          neighbours = get2DNodes(i)
        } else if (topology == "line") {
          neighbours = getLineNodes(i)
        } else if (topology == "imp2D") {
          neighbours = getImp2DNodes(i)
        }
        nodesArr(i) ! GossipWorker.AddNeighbour(neighbours)
      }
    }
  }

  def getFullNetworkNodes(nodeNo: Int): ArrayBuffer[ActorRef] = {
    val arr = nodesArr.clone()
    arr.remove(nodeNo)
    return arr
  }

  def getLineNodes(nodeNo: Int): ArrayBuffer[ActorRef] = {
    val arr = ArrayBuffer.empty[ActorRef]
    if (nodeNo == 0)
      arr += nodesArr(1)
    else if (nodeNo == nodesArr.length - 1)
      arr += nodesArr(nodesArr.length - 2)
    else if (nodeNo > 0 && nodeNo < nodesArr.length) {
      arr += nodesArr(nodeNo - 1)
      arr += nodesArr(nodeNo + 1)
    }
    return arr
  }

  def get2DNodes(nodeNo: Int): ArrayBuffer[ActorRef] = {
    val arr = ArrayBuffer.empty[ActorRef]
    val size = Math.sqrt(noOfNodes.toDouble).toInt

    var x = nodeNo / size // row
    var y = nodeNo % size // column

    if (x - 1 >= 0) {
      arr += nodesArr(((x - 1) * size + y).toInt)
    }
    if (x + 1 < size) {

      arr += nodesArr(((x + 1) * size + y).toInt)
    }
    if (y - 1 >= 0) {
      arr += nodesArr((x * size + y - 1).toInt)
    }
    if (y + 1 < size) {
      arr += nodesArr((x * size + y + 1).toInt)
    }
    return arr
  }

  def getImp2DNodes(nodeNo: Int): ArrayBuffer[ActorRef] = {
    val arr = get2DNodes(nodeNo)
    val tmpArr = arr.map(a => a.path.name.drop(6).toInt) // drop worker prefix from name of neighbor to get node name
    tmpArr += nodeNo // add self node to exclude list when selecting random

    var tmp = 1
    do {
      tmp = rand.nextInt(nodesArr.length)
    } while (tmpArr.contains(tmp))
    arr += nodesArr(tmp)
    return arr
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

  object GossipWorker {
    case class AddNeighbour(arr: ArrayBuffer[ActorRef])
    case class RemoveNeighbour(arr: ArrayBuffer[ActorRef])
    case object Done
    case object Failed
    case object Stop
    case object StopAck
  }

  class GossipWorker() extends Actor {
    var neighbors = ArrayBuffer.empty[ActorRef]
    def receive = LoggingReceive {
      case GossipWorker.AddNeighbour(arr) =>
        neighbors ++= arr
      case GossipWorker.Stop =>
        sender ! GossipWorker.StopAck
        context.stop(self)
      case _ => sender ! GossipWorker.Failed
    }
  }

  class PushSumWorker() extends GossipWorker {

    override def receive = LoggingReceive {
      case GossipWorker.AddNeighbour(arr) =>
        neighbors ++= arr
      case _ => sender ! GossipWorker.Failed
    }
  }

}