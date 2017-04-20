package stackoverflow

import org.scalatest.{FunSuite, BeforeAndAfterAll}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import java.net.URL
import java.nio.channels.Channels
import java.io.File
import java.io.FileOutputStream

@RunWith(classOf[JUnitRunner])
class StackOverflowSuite extends FunSuite with BeforeAndAfterAll {


  lazy val testObject = new StackOverflow {

    @transient
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("StackOverflow")
    @transient
    val sc: SparkContext = new SparkContext(conf)

    override val langs =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

    override def langSpread = 50000

    override def kmeansKernels = 45

    override def kmeansEta: Double = 20.0D

    override def kmeansMaxIterations = 120
  }

  test("testObject can be instantiated") {
    val instantiatable = try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }

  test("Should calculate high score") {
    val question1 = Posting(1, 1, None, None, 0, None)
    val answer1_1 = Posting(2, 10, None, Some(1), 1, None)
    val answer1_2 = Posting(2, 11, None, Some(1), 2, None)
    val answer1_3 = Posting(2, 12, None, Some(1), 3, None)

    val question2 = Posting(1, 2, None, None, 1, None)
    val answer2_1 = Posting(2, 20, None, Some(2), 3, None)
    val answer2_2 = Posting(2, 21, None, Some(2), 7, None)
    val answer2_3 = Posting(2, 22, None, Some(2), 1, None)

    val rdd = List((1, List((question1, answer1_1), (question1, answer1_2), (question1, answer1_3))),
      (2, List((question2, answer2_1), (question2, answer2_2), (question2, answer2_3))))

    val rez = testObject.scoredPostings(testObject.sc.parallelize(rdd))

    rez.foreach(r => println(r))
  }

  test("Should calculate high score real") {
    val lines = testObject.sc.textFile("src/main/resources/stackoverflow/stackoverflow.csv")
    val raw = testObject.rawPostings(lines)
    val grouped = testObject.groupedPostings(raw)
    val scored = testObject.scoredPostings(grouped)
    scored.take(20).foreach(f=>println(f))
    assert(scored.count() == 2121822, "Incorrect number of scores: " + scored.count())
  }

  test("Should calculate vectors") {
    val lines = testObject.sc.textFile("src/main/resources/stackoverflow/stackoverflow.csv")
    val raw = testObject.rawPostings(lines)
    val grouped = testObject.groupedPostings(raw)
    val scored = testObject.scoredPostings(grouped)
    val vectors = testObject.vectorPostings(scored)
    assert(vectors.count() == 2121822, "Incorrect number of vectors: " + vectors.count())
  }

  test("Should calculate vector sample") {
    val question1 = Posting(1, 1, None, None, 0, Some("Java"))
    val question2 = Posting(1, 2, None, None, 1, Some("Python"))

    val rdd = List((question1, 10), (question2, 20))

    val rez = testObject.vectorPostings(testObject.sc.parallelize(rdd))

    rez.foreach(r => println(r))
  }


  test("Should calculate cluster result") {

    val rdd = testObject.sc.parallelize(List((50000, 10), (300000, 20), (50000, 40), (100000, 40)))
    val means = Array((50000,10), (300000, 25))

    val rez = testObject.clusterResults(means, rdd)

    testObject.printResults(rez)
  }

  test("Should calculate cluster result simple") {

    val means = Array((50000,10), (300000, 25),(50000,100),(50000,20), (400000, 1000))

    val rez = testObject.getResults(means)

    testObject.printResults(Array(rez))
  }


}
