package old.main

import scala.util.control.Breaks
import java.security.MessageDigest
import akka.actor.Actor
import akka.event.LoggingReceive
import akka.actor.ActorRef

object StringIncrementer {
  def next(s: String): String = {
    val length = s.length
    var c = s.charAt(length - 1)
    if (c == 'z' || c == 'Z') return if (length > 1) next(s.substring(0, length - 1)) + 'A' else "AA"
    s.substring(0, length - 1) + (c.toInt + 1).toChar
  }

  def incrementByN(s: String, n: Int): String = {
    var i = 0
    var temp = s
    for (i <- 1 to n) {
      temp = next(temp)
    }
    return temp
  }
}

object SHA256 {
  private val sha = MessageDigest.getInstance("SHA-256")
  def hash(s: String): String = {
    sha.digest(s.getBytes).foldLeft("")((s: String, b: Byte) => s +
      Character.forDigit((b & 0xf0) >> 4, 16) +
      Character.forDigit(b & 0x0f, 16))
  }
}

object Worker {
  case class Compute(str: String, ctr: Int)
  case class ValidBitCoin(inputStr: String, outputStr: String)
  case object Done
  case object Failed
}

class Worker(leadingZeros: Int) extends Actor {
  import Worker._
  var tempStartString = ""
  private val prefix = "asrivastava"

  def receive = LoggingReceive {
    case Compute(str, ctr) => ComputeImpl(str, ctr, sender)
    case _ => sender ! Failed
  }

  def ComputeImpl(str: String, ctr: Int, sender: ActorRef) {
    var i = 1
    tempStartString = str
    for (i <- 1 to ctr) {
      var hex = SHA256.hash(prefix + tempStartString)
      var result = checkLeadingZeros(hex, leadingZeros)
      if (result) {
        sender ! ValidBitCoin(prefix + tempStartString, hex)
      }
      tempStartString = StringIncrementer.next(tempStartString)
    }
  }

  def checkLeadingZeros(s: String, numberOfZeros: Int): Boolean = {
    var a = 0
    var flag = true
    val loop = new Breaks;
    loop.breakable {
      for (i <- 0 to numberOfZeros - 1) {
        if (s.charAt(i) == '0') {
          flag = true
        } else {
          flag = false
          loop.break
        }
      }
    }
    if (flag == false) {
      return false
    } else return true
  }
}