package org.jyp.graphx

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

@Test
class RDDTest {


  def getConfig: SparkConf = {
    new SparkConf().setMaster("local[2]").setAppName("testPairRDD")
  }

  @Test
  def testPairRDD: Unit = {
    val spark = new SparkContext(getConfig)
    val r = spark.makeRDD(Array("Apples", "Bananas", "Oranges"))
    val pairrdd = r.map(x => (x.substring(0, 1), x)).cache()
    pairrdd.foreach(println)
    spark.stop()
    assert(pairrdd != null)

  }

  @Test
  def testZip: Unit = {
    val spark = new SparkContext(getConfig)
    val r1 = spark.makeRDD(Array(10, 20, 30, 40))
    val r2 = spark.makeRDD(Array(1, 2, 3, 4))
    val r3 = r1.zip(r2)
    r3.map((a) => a._1 + a._2).foreach(println)
    spark.stop()

  }
}
