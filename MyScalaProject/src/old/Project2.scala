import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.util.Random
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.Terminated
import akka.actor.actorRef2Scala
import akka.event.LoggingReceive
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory

// Think of tracking the messages being recd by the actors and shutdown entire system when recd atleast once.

object Project2N {
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

      val system = ActorSystem("Gossip", ConfigFactory.load(ConfigFactory.parseString("""{ "akka" : { "log-dead-letters" : 1000 } } """)))
      val watcher = system.actorOf(Props(new Watcher(numNodes, topo, algo)), name = "Watcher")
      watcher ! Watcher.Initiate
    }
  }

  object Watcher {
    // Used by others to register an Actor for watching
    case class Terminate(ref: ActorRef)
    case object Initiate
    case class GetRandomNode(ref: ActorRef)
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
      if (!neighbours.isEmpty) {
        nodesArr(i) ! GossipWorker.AddNeighbour(neighbours)
      }
    }
    val startTime = System.currentTimeMillis()
    // end of constructor

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

    def getRandom(ref: ActorRef): ActorRef = {
      if (nodesArr.length == 1) {
        return ref
      } else {
        var tmp = 0
        do {
          tmp = rand.nextInt(nodesArr.length)
        } while (nodesArr(tmp) == ref)
        return nodesArr(tmp)
      }
    }

    final def receive = {
      case Terminate(ref) =>
        nodesArr -= ref
        println("No of nodes remaining after " + ref.path.name + "is " + nodesArr.length)
        if (nodesArr.isEmpty) {
          val finalTime = System.currentTimeMillis()
          println(finalTime - startTime)
          context.system.shutdown
        }
      case Initiate => nodesArr(0) ! GossipWorker.Gossip
      case GetRandomNode(ref) =>
        sender ! getRandom(ref)
      case _ => println("FAILED HERE")
    }

  }

  object GossipWorker {
    case class AddNeighbour(arr: ArrayBuffer[ActorRef])
    case object RemoveNeighbour
    case class RandomGuy(randomGuy: ActorRef)
    case class PushSumMsg(s: Int, w: Int)
    case object Gossip
    case object Failed
  }

  class GossipWorker(topology: String, algorithm: String) extends Actor {
    import context._
    import GossipWorker._
    var neighbors = ArrayBuffer.empty[ActorRef]
    var randomInwardGuy = ArrayBuffer.empty[ActorRef]
    var watcherRef: ActorRef = null
    var count = 0
    var rand = new Random(System.currentTimeMillis())
    val name = self.path.name

    def gossipFullNetwork: Receive = {
      case Gossip =>
        count += 1;
        if (count == 10) {
          implicit val timeout = new Timeout(5 seconds)
          val future = watcherRef ? Watcher.GetRandomNode
          val result = Await.result(future, timeout.duration).asInstanceOf[ActorRef]
          result ! Gossip
          context.stop(self)
        } else {
          implicit val timeout = new Timeout(5 seconds)

          val future = watcherRef ? Watcher.GetRandomNode
          val result = Await.result(future, timeout.duration).asInstanceOf[ActorRef]
          result ! Gossip
        }
      case _ => sender ! Failed
    }

    def pushSumFullNetwork: Receive = {
      case Gossip =>
        count += 1;
        if (count == 10) {
          implicit val timeout = new Timeout(5 seconds)
          val future = watcherRef ? Watcher.GetRandomNode(self)
          val result = Await.result(future, timeout.duration).asInstanceOf[ActorRef]
          watcherRef ! Watcher.Terminate(self)
          result ! Gossip
          context.stop(self)
        } else {
          implicit val timeout = new Timeout(10 seconds)
          val future = watcherRef ? Watcher.GetRandomNode(self)
          val result = Await.result(future, timeout.duration).asInstanceOf[ActorRef]
          result ! Gossip
        }
      case _ => sender ! Failed
    }

    def gossipNetwork: Receive = {
      case AddNeighbour(arr) =>
        neighbors ++= arr
      case RemoveNeighbour =>
        neighbors -= sender
        if (neighbors.length == 0) {
          randomInwardGuy.foreach(a => a ! RemoveNeighbour)
          context.stop(self)
        }
      case RandomGuy(randomGuy) =>
        randomInwardGuy += randomGuy
      case Gossip =>
        count += 1;
        val result = neighbors(rand.nextInt(neighbors.length))
        if (count == 10) {
          neighbors.foreach(a => a ! RemoveNeighbour)
          randomInwardGuy.foreach(a => a ! RemoveNeighbour)
          result ! Gossip
          context.stop(self)
        } else {
          result ! Gossip
        }
      case _ => sender ! Failed
    }

    def pushSumNetwork: Receive = {
      case AddNeighbour(arr) =>
        neighbors ++= arr
      case RemoveNeighbour =>
        neighbors -= sender
        if (neighbors.length == 0) {
          randomInwardGuy.foreach(a => a ! RemoveNeighbour)
          context.stop(self)
        }
      case RandomGuy(randomGuy) =>
        randomInwardGuy += randomGuy
      case Gossip =>
        count += 1;
        val result = neighbors(rand.nextInt(neighbors.length))
        if (count == 10) {
          neighbors.foreach(a => a ! RemoveNeighbour)
          randomInwardGuy.foreach(a => a ! RemoveNeighbour)
          result ! Gossip
          context.stop(self)
        } else {
          result ! Gossip
        }
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