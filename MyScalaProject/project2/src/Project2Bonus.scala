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

// Think of tracking the messages being recd by the actors and shutdown entire system when recd atleast once.

object Project2Bonus {
  def main(args: Array[String]) {
    // exit if arguments not passed as command line param.
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

      // convert no. of nodes into Perfect Square if topology is 2D or imp2D
      if (topo == "2D" || topo == "imp2D") {
        var tmp = Math.sqrt(numNodes.toDouble).ceil
        tmp = tmp * tmp
        numNodes = tmp.toInt
      }

      var failureRate = 1
      var NodeTimeoutCounter = 4000 // Total TimeOut in milliseconds = NodeTimeoutCounter * HeartBeat

      // create actor system and a watcher actor
      val system = ActorSystem("Gossip")
      val watcher = system.actorOf(Props(new Watcher(numNodes, topo, algo, NodeTimeoutCounter)), name = "Watcher")
      watcher ! Watcher.NodeFailure(failureRate)
      watcher ! Watcher.Initiate
    }
  }

  object Watcher {
    case class Terminate(ref: ActorRef, sw: Double = 0.0)
    case object Initiate
    case object GetRandomNode
    case object Notify
    case class NodeFailure(failureRate: Int)
  }

  class Watcher(noOfNodes: Int, topology: String, algorithm: String, ctr: Int) extends Actor {
    import Watcher._
    import context._
    var startTime = System.currentTimeMillis()
    var total = 0

    // keep track of actors and their neighbors.
    val nodesArr = ArrayBuffer.empty[ActorRef]
    val rand = new Random(System.currentTimeMillis())

    var noOfActorsRecdMsg = 0

    // create array of all nodes (actors)    
    for (i <- 0 to noOfNodes - 1) {
      var node = actorOf(Props(new GossipWorker(ctr)), name = "Worker" + i)
      node ! GossipWorker.Init(algorithm, topology)
      nodesArr += node
    }

    // For anything apart from a full n/w, compute neighbors and store them.
    for (i <- 0 to noOfNodes - 1) {
      var neighbours = ArrayBuffer.empty[ActorRef]
      if (topology == "2D") {
        neighbours = get2DNodes(i)
      } else if (topology == "line") {
        neighbours = getLineNodes(i)
      } else if (topology == "imp2D") {
        neighbours = getImp2DNodes(i)
      }
      if (!neighbours.isEmpty) {
        nodesArr(i) ! GossipWorker.AddNeighbour(neighbours)
      }
    }
    // end of constructor

    // Get valid neighbors in a Line n/w Topology for a given node no.
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

    // Get valid neighbors in a 2D n/w Topology for a given node no.
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

    // Get valid neighbors in an imperfect 2D n/w Topology for a given node no.
    def getImp2DNodes(nodeNo: Int): ArrayBuffer[ActorRef] = {
      val arr = get2DNodes(nodeNo)
      val tmpArr = arr.map(a => a.path.name.drop(6).toInt) // drop worker prefix from name of neighbor to get node numbers.
      tmpArr += nodeNo // add self node to exclude list when selecting random

      // select a random node as a fifth guy.
      var tmp = 1
      do {
        tmp = rand.nextInt(nodesArr.length)
      } while (tmpArr.contains(tmp))
      arr += nodesArr(tmp)
      return arr
    }

    // Get random neighbor in a full n/w Topology for a given node no.
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

    // Receive block for the Watcher.
    final def receive = {
      // send message to the first node to initiate after setting start time.
      case Initiate =>
        startTime = System.currentTimeMillis()
        if (algorithm == "push-sum") {
          nodesArr(0) ! GossipWorker.PushSumMsg(0.0, 1.0)
        } else {
          nodesArr(0) ! GossipWorker.Gossip
        }

      case Notify =>
        noOfActorsRecdMsg += 1
        //println("No of Actors converged " + noOfActorsRecdMsg)
        if (noOfActorsRecdMsg == noOfNodes - total) {
          var convergenceTime = System.currentTimeMillis()
          println("convergence time " + (convergenceTime - startTime))
        }

      // get random node to send the message to.
      case GetRandomNode =>
        getRandomNode(sender)

      // When Actors send Terminate Message to Watcher to remove from network.
      case Terminate(ref, sw) =>
        nodesArr -= ref
        val finalTime = System.currentTimeMillis()
        println("No of Actors Alive " + nodesArr.length + " wisth current Val: " + sw + " and time: " + (finalTime - startTime))
        // when all actors are down, shutdown the system.
        if (nodesArr.isEmpty) {
          println("Final:" + (finalTime - startTime))
          println("No Of actors that converged : " + noOfActorsRecdMsg)
          println("No of actors that did not converge " + (noOfNodes - total - noOfActorsRecdMsg))
          context.system.shutdown
        }

      case NodeFailure(failureRate) =>
        total = nodesArr.length * failureRate / 100
        var fc = total

        while (fc > 0 && nodesArr.length > 0) {
          var tmp = nodesArr(rand.nextInt(nodesArr.length))
          tmp ! GossipWorker.Stop
          nodesArr -= tmp
          fc -= 1
        }

      case _ => println("FAILED HERE")
    }
  }

  object GossipWorker {
    case class AddNeighbour(arr: ArrayBuffer[ActorRef])
    case object RemoveNeighbour
    case class GossipNode(result: ActorRef)
    case class PushSumMsg(s: Double, w: Double)
    case object Gossip
    case object Stop
    case object SendMessage
    case class Init(algo: String, topo: String)
  }

  class GossipWorker(StopCtr: Int) extends Actor {
    import context._
    import GossipWorker._
    var watcherRef: ActorRef = null
    var count = 0
    var s: Double = self.path.name.drop(6).toDouble
    var w: Double = 1
    var sw: Double = 0
    var prevsw: Double = 0
    var consecutive: Int = 0
    val neighbors = ArrayBuffer.empty[ActorRef]
    val rand = new Random(System.currentTimeMillis())
    var isAlive = false
    var cancellable = system.scheduler.schedule(2 seconds, 5 milliseconds, self, SendMessage)
    var timeoutCounter = 0

    def stop() {
      cancellable.cancel
      isAlive = false
      count = 10
      watcherRef ! Watcher.Terminate(self)
    }

    // Receive Block for a normal Gossip Message
    def gossipNetwork: Receive = {
      case AddNeighbour(arr) =>
        neighbors ++= arr

      case RemoveNeighbour =>
        neighbors -= sender

      case Gossip =>
        count += 1;
        if (count == 1) {
          isAlive = true
          watcherRef ! Watcher.Notify
        }
        if (isAlive) {
          if (count == 10) {
            cancellable.cancel
            isAlive = false
            neighbors.foreach(a => a ! RemoveNeighbour)
            watcherRef ! Watcher.Terminate(self)
          }
        } else {
          sender ! RemoveNeighbour
        }

      case SendMessage =>
        timeoutCounter += 1
        if (isAlive) {
          if (neighbors.length != 0) {
            val result = neighbors(rand.nextInt(neighbors.length))
            result ! Gossip
          } else {
            cancellable.cancel
            isAlive = false
            watcherRef ! Watcher.Terminate(self)
          }
        } else {
          if (timeoutCounter > StopCtr) {
            cancellable.cancel
            watcherRef ! Watcher.Terminate(self)
          }
        }

      case Stop =>
        stop()

      case _ => println("FAILED")
    }

    // Receive Block for a normal Gossip Message
    def gossipFullNetwork: Receive = {
      case Gossip =>
        count += 1;
        if (count == 1) {
          isAlive = true
          watcherRef ! Watcher.Notify
        }
        if (isAlive) {
          if (count == 10) {
            cancellable.cancel
            isAlive = false
            watcherRef ! Watcher.Terminate(self)
          }
        }

      case SendMessage =>
        timeoutCounter += 1
        if (isAlive) {
          watcherRef ! Watcher.GetRandomNode
        } else {
          if (timeoutCounter > StopCtr) {
            cancellable.cancel
            watcherRef ! Watcher.Terminate(self)
          }
        }

      case GossipNode(ref) =>
        ref ! Gossip

      case Stop =>
        stop()

      case _ => println("FAILED")
    }

    def pushSumFullNetwork: Receive = {
      case PushSumMsg(a, b) =>
        count += 1;
        if (count == 1) {
          isAlive = true
          watcherRef ! Watcher.Notify
        }
        if (isAlive) {
          s = s + a;
          w = w + b;
          sw = s / w;
        }

      case SendMessage =>
        timeoutCounter += 1
        if (isAlive) {
          if ((prevsw - (sw)).abs > .0000000001) {
            consecutive = 0;
          } else {
            consecutive += 1;
          }
          if (consecutive == 3) {
            cancellable.cancel
            isAlive = false
            watcherRef ! Watcher.Terminate(self, sw)
          } else {
            s = s / 2.0
            w = w / 2.0
            prevsw = sw
            watcherRef ! Watcher.GetRandomNode
          }
        } else {
          if (timeoutCounter > StopCtr) {
            cancellable.cancel
            watcherRef ! Watcher.Terminate(self)
          }
        }

      case GossipNode(ref) =>
        ref ! PushSumMsg(s, w)

      case Stop =>
        stop()

      case _ => println("FAILED")
    }

    def pushSumNetwork: Receive = {
      case AddNeighbour(arr) =>
        neighbors ++= arr

      case RemoveNeighbour =>
        neighbors -= sender

      case PushSumMsg(a, b) =>
        count += 1;
        if (count == 1) {
          isAlive = true
          watcherRef ! Watcher.Notify
        }
        if (isAlive) {
          s = s + a;
          w = w + b;
          sw = s / w;
        } else {
          sender ! RemoveNeighbour
        }

      case SendMessage =>
        timeoutCounter += 1
        if (isAlive) {
          if ((prevsw - (sw)).abs > .0000000001) {
            consecutive = 0;
          } else {
            consecutive += 1;
          }
          if (consecutive == 3) {
            cancellable.cancel
            isAlive = false
            neighbors.foreach(a => a ! RemoveNeighbour)
            watcherRef ! Watcher.Terminate(self, sw)
          } else {
            s = s / 2.0
            w = w / 2.0
            prevsw = sw
            if (neighbors.length != 0) {
              val result = neighbors(rand.nextInt(neighbors.length))
              result ! PushSumMsg(s, w)
            } else {
              cancellable.cancel
              isAlive = false
              watcherRef ! Watcher.Terminate(self, sw)
            }
          }
        } else {
          if (timeoutCounter > StopCtr) {
            cancellable.cancel
            watcherRef ! Watcher.Terminate(self)
          }
        }

      case Stop =>
        stop()

      case _ => println("FAILED")
    }

    def receive = LoggingReceive {
      case Init(algorithm, topology) =>
        watcherRef = sender
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