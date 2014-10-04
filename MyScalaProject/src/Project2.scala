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

      val system = ActorSystem("Gossip", ConfigFactory.load(ConfigFactory.parseString("""{ "akka" : { "log-dead-letters" : 1000 } } """)))
      val watcher = system.actorOf(Props(new Watcher(numNodes, topo, algo)), name = "Watcher")
      watcher ! Watcher.Initiate
    }
  }

  object Watcher {
    // Used by others to register an Actor for watching
    case class Terminate(ref: ActorRef)
    case object Initiate
    case object GetRandomNode
    case class ReInstate(s: Double, w: Double)
    case object ReinstateGossip
  }

  class Watcher(noOfNodes: Int, topology: String, algorithm: String) extends Actor {
    import Watcher._
    import context._
    var startTime = System.currentTimeMillis()

    // keep track of what we have created and are watching.
    val nodesArr = ArrayBuffer.empty[ActorRef]
    val neighborArr = ArrayBuffer.empty[ArrayBuffer[ActorRef]]
    val rand = new Random(System.currentTimeMillis())

    // create array of all nodes (actors)    
    for (i <- 0 to noOfNodes - 1) {
      var node = actorOf(Props(new GossipWorker()), name = "Worker" + i)
      node ! GossipWorker.Init(algorithm)
      nodesArr += node
    }

    for (i <- 0 to noOfNodes - 1) {
      var neighbours = ArrayBuffer.empty[ActorRef]
      if (topology == "2D") {
        neighbours = get2DNodes(i)
      } else if (topology == "line") {
        neighbours = getLineNodes(i)
      } else if (topology == "imp2D") {
        neighbours = getImp2DNodes(i)
      }
      neighborArr += neighbours
    }
    // end of constructor

    def getNode(ref: ActorRef): Unit = topology match {
      case "full" => getRandomNode(ref)
      case "2D" | "line" | "imp2D" =>
        var index = ref.path.name.drop(6).toInt
        var tmp = neighborArr(index)
        var found = false

        while (!tmp.isEmpty && !found) {
          var result = tmp(rand.nextInt(tmp.length))
          if (nodesArr.contains(result)) {
            found = true
            ref ! GossipWorker.GossipNode(result)
          } else {
            tmp -= result
          }
        }
        if (!found) {
          ref ! GossipWorker.Stop
        }
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

    def getRandomNode(ref: ActorRef): Unit = {
      // send stop message to self if u are the only one left.
      if (nodesArr.length == 1) {
        ref ! GossipWorker.Stop
      } else {
        var tmp = 0
        // loop till u find a reference other than ur self.
        do {
          tmp = rand.nextInt(nodesArr.length)
        } while (nodesArr(tmp) == ref)
        // send the reference to the actor which requested the node.
        ref ! GossipWorker.GossipNode(nodesArr(tmp))
      }
    }

    final def receive = {
      // send message to the first node to initiate after setting start time.
      case Initiate =>
        startTime = System.currentTimeMillis()
        if (algorithm == "push-sum") {
          nodesArr(0) ! GossipWorker.PushSumMsg(0.0, 1.0)
        } else {
          nodesArr(0) ! GossipWorker.Gossip
        }

      // get random node to send the message to.
      case GetRandomNode =>
        getNode(sender)

      case ReInstate(s, w) =>
        nodesArr(rand.nextInt(nodesArr.length)) ! GossipWorker.PushSumMsg(s, w)

      case ReinstateGossip =>
        nodesArr(rand.nextInt(nodesArr.length)) ! GossipWorker.Gossip

      // Send Terminate Message to this actor to remove from network.
      case Terminate(ref) =>
        nodesArr -= ref
        if (nodesArr.isEmpty) {
          val finalTime = System.currentTimeMillis()
          println(finalTime - startTime)
          context.system.shutdown
        }

      case _ => println("FAILED HERE")
    }

  }

  object GossipWorker {
    case class GossipNode(result: ActorRef)
    case class PushSumMsg(s: Double, w: Double)
    case object Gossip
    case object Stop
    case object Failed
    case class Init(algo: String)
  }

  class GossipWorker() extends Actor {
    import context._
    import GossipWorker._
    var watcherRef: ActorRef = null
    var count = 0
    var s: Double = self.path.name.drop(6).toDouble
    var w: Double = 1
    var sw: Double = 0
    var prevsw: Double = 0
    var consecutive: Int = 0

    def gossipNetwork: Receive = {
      // when receive a msg, ask for a node to send to.
      case Gossip =>
        watcherRef ! Watcher.GetRandomNode
      // on receiving the node, forward the message accordingly.
      case GossipNode(result) =>
        count += 1;
        if (count == 10) {
          watcherRef ! Watcher.Terminate(self)
          result ! Gossip
          context.stop(self)
        } else {
          result ! Gossip
        }
      // when I receive stop request from master itself.
      case Stop =>
        watcherRef ! Watcher.Terminate(self)
        watcherRef ! Watcher.ReinstateGossip
        context.stop(self)
      case _ => sender ! Failed
    }

    def pushSumNetwork: Receive = {
      // when receive a msg, ask for a node to send to.
      case PushSumMsg(a, b) =>
        prevsw = s / w
        s = s + a;
        w = w + b;
        sw = s / w;
        watcherRef ! Watcher.GetRandomNode

      // on receiving the node, forward the message accordingly.      
      case GossipNode(result) =>
        if ((prevsw - (sw)).abs > .0000000001) {
          consecutive = 0;
        } else {
          consecutive += 1;
        }
        if (consecutive == 3) {
          watcherRef ! Watcher.Terminate(self)
          result ! PushSumMsg(s / 2, w / 2)
          context.stop(self)
        } else {
          result ! PushSumMsg(s / 2, w / 2)
        }

      // when I receive stop request from master itself.
      case Stop =>
        watcherRef ! Watcher.Terminate(self)
        watcherRef ! Watcher.ReInstate(s / 2, w / 2)
        context.stop(self)
      case _ => sender ! Failed
    }

    def receive = LoggingReceive {
      case Init(algorithm) =>
        watcherRef = sender
        // if algorithm type is push sum, use a certain type of object
        if (algorithm == "push-sum") {
          become(pushSumNetwork)
        } else {
          become(gossipNetwork)
        }
      case _ => println("FAILED")
    }
  }
}