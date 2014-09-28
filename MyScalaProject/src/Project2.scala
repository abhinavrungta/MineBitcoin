import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import com.typesafe.config.ConfigFactory
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.Terminated
import akka.event.LoggingReceive
import scala.concurrent.Await
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._

object Project2 {
  def main(args: Array[String]) {
    // exit if argument not passed as command line param
    if (args.length < 3) {
      println("INVALID NO OF ARGS.  USAGE :")
      println("1. Number of Nodes")
      println("2. Topology - full, 2D, line, imp2D")
      println("3. Protocol - push-sum, gossip")
      System.exit(1)
    } else if (args.length == 3) {
      var numNodes = args(0).toInt
      var topo = args(1)
      var algo = args(2)

      // convert no of nodes into Perfect Square if topology is 2D or imp2D
      if (topo == "2D" || topo == "imp2D") {
        var tmp = Math.sqrt(numNodes.toDouble).ceil
        tmp = tmp * tmp
        numNodes = tmp.toInt
      }

      // create actor system and a watcher actor
      val system = ActorSystem("Gossip")
      val watcher = system.actorOf(Props(new Watcher(numNodes, topo, algo)), name = "Watcher")
      watcher ! Watcher.Initiate
    }
  }

  object Watcher {
    // Used by others to register an Actor for watching
    case class WatchMe(ref: ActorRef)
    case object Initiate
    case object GetRandomNode
  }

  class Watcher(noOfNodes: Int, topology: String, algorithm: String) extends Actor {
    import Watcher._
    import context._

    // keep track of what we have created and are watching.
    val nodesArr = ArrayBuffer.empty[ActorRef]
    val rand = new Random(System.currentTimeMillis())

    // create array of all nodes (actors)    
    for (i <- 0 to noOfNodes - 1) {
      var node = actorOf(Props(new GossipWorker(topology, algorithm)), name = "Worker" + i)
      node ! "init"
      nodesArr += node
      context.watch(node)
    }

    // send list of neighbors to all nodes (actors) 
    for (i <- 0 to noOfNodes - 1) {
      var neighbours = ArrayBuffer.empty[ActorRef]
      if (topology == "2D") {
        neighbours = get2DNodes(i)
      } else if (topology == "line") {
        neighbours = getLineNodes(i)
      } else if (topology == "imp2D") {
        neighbours = getImp2DNodes(i)
        // the last element in the array is the random guy. Send this guy the inward bound reference.
        var randomNeighbour = neighbours.last
        randomNeighbour ! GossipWorker.RandomGuy(nodesArr(i))
      }
      nodesArr(i) ! GossipWorker.AddNeighbour(neighbours)
    }
    // end of constructor
    final def receive = {
      case WatchMe(ref) =>
        context.watch(ref)
        nodesArr += ref
      case Terminated(ref) =>
        nodesArr -= ref
        if (nodesArr.isEmpty) {
          context.system.shutdown
        }
      case Initiate =>
      case GetRandomNode => getRandom(sender)
      case _ => println("FAILED")

    }

    def getRandom(ref: ActorRef): ActorRef = {
      var selfIndex = ref.path.name.drop(6).toInt
      var tmp = 0
      var ctr = 0
      do {
        tmp = rand.nextInt(nodesArr.length)
        ctr += 1
      } while (tmp != selfIndex && ctr < 5)
      return nodesArr(tmp)
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
  }

  object GossipWorker {
    case class AddNeighbour(arr: ArrayBuffer[ActorRef])
    case class RemoveNeighbour(arr: ArrayBuffer[ActorRef])
    case class RandomGuy(randomGuy: ActorRef)
    case class PushSumMsg(s: Int, w: Int)
    case object Gossip
    case object Failed
  }

  class GossipWorker(topology: String, algorithm: String) extends Actor {
    import context._
    import GossipWorker._
    var neighbors = ArrayBuffer.empty[ActorRef]
    var randomInwardGuy: ActorRef = null
    var watcherRef: ActorRef = null

    def gossipFullNetwork: Receive = {
      case Gossip =>
        implicit val timeout = new Timeout(5 seconds)
        val future = watcherRef ? Watcher.GetRandomNode
        val result = Await.result(future, timeout.duration).asInstanceOf[ActorRef]
        result ! "gossip"
        stop(self)
      case _ => sender ! Failed
    }

    def pushSumFullNetwork: Receive = {
      case "" =>
      case _ => sender ! Failed
    }

    def gossipNetwork: Receive = {
      case AddNeighbour(arr) =>
        neighbors ++= arr
      case RemoveNeighbour(arr) =>
        neighbors --= arr
      case RandomGuy(randomGuy) => randomInwardGuy = randomGuy
      case _ => sender ! Failed
    }

    def pushSumNetwork: Receive = {
      case AddNeighbour(arr) =>
        neighbors ++= arr
      case RemoveNeighbour(arr) =>
        neighbors --= arr
      case RandomGuy(randomGuy) => randomInwardGuy = randomGuy
      case _ => sender ! Failed
    }

    def receive = LoggingReceive {
      case "init" =>
        watcherRef = sender
        // if algorithm type is push sum, use a certain type of object
        if (topology == "full") {
          if (algorithm == "push-sum") {
            become(pushSumFullNetwork)
          } else {
            become(gossipFullNetwork)
          }
        } else {
          if (algorithm == "push-sum") {
            become(pushSumNetwork)
          } else {
            become(gossipNetwork)
          }
        }
      case _ => println("FAILED")
    }
  }
}