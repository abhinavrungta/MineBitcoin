package project4.src

import scala.annotation.tailrec
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

  // get probability of a predicate for the distribution defined, by taking 10000 (default) samples
  def probability(predicate: A => Boolean): Double = {
    val N = 10000 // default empirical value of 10000 samples
    this.sample(N).count(predicate).toDouble / N
  }

  // filter distribution values from sample based on predicate.
  def given(predicate: A => Boolean): Distribution[A] = new Distribution[A] {
    @tailrec
    override def get = {
      val a = self.get
      if (predicate(a)) a else this.get
    }
  }

  def repeat(n: Int): Distribution[List[A]] = new Distribution[List[A]] {
    override def get = {
      List.fill(n)(self.get)
    }
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

  /*println(uniform.probability(_ < 0.6))
    bernoulli(0.8).sample(10).foreach(println)

    val die = discreteUniform((1 to 6))
    println(discreteUniform((1 to 6)).probability(_ == 6))
    println(die.given(_ % 2 == 0).probability(_ == 4))
    val dice = die.repeat(2).map(_.sum)
*/
}