package project4.src

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

  // define a true/false method to modify uniform random distribution.
  def tf(p: Double): Distribution[Boolean] = {
    uniform.map(_ < p)
  }

  // define a bernoulli method to output 0/1 on top of tf method to modify uniform random distribution.
  def bernoulli(p: Double): Distribution[Int] = {
    tf(p).map(b => if (b) 1 else 0)
  }

  // randomly pick a value and map it to a given set of values.
  def discreteUniform[A](values: Iterable[A]): Distribution[A] = {
    val vec = values.toVector
    uniform.map(x => vec((x * vec.length).toInt))
  }

  def exponential(l: Double): Distribution[Double] = {
    for {
      x <- uniform
    } yield math.log(x) * (-1 / l)
  }
}