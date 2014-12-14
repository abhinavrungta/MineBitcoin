package main.scala

import scala.collection.mutable.ArrayBuffer

trait Distribution[A] {
  self =>
  def get: A

  def sample(n: Int): ArrayBuffer[A] = {
    ArrayBuffer.fill(n)(this.get)
  }

  def map[B](f: A => B): Distribution[B] = new Distribution[B] {
    override def get = f(self.get)
  }
}

class PDF {
  // create a uniform random distribution object.
  val uniform = new Distribution[Double] {
    // define the get method for Distribution object.
    private val rand = new java.util.Random()
    override def get = rand.nextDouble()
  }

  val gaussian = new Distribution[Double] {
    // define the get method for Distribution object.
    private val rand = new java.util.Random()
    override def get = rand.nextGaussian()
  }

  def exponential(l: Double): Distribution[Double] = {
    for {
      x <- uniform
    } yield math.log(x) * (-1 / l)
  }
}