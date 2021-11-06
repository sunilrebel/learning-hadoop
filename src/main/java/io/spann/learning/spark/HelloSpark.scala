package io.spann.learning.spark

import org.apache.spark.{SparkContext, SparkConf}

object HelloSpark {

  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf().setAppName("HelloSpark").setMaster("spark://192.168.56.101:7077")
    val sc: SparkContext = new SparkContext(conf)

    val rdd = sc.parallelize(List(1,2,3,4))
    rdd.saveAsTextFile("hdfs://quickstart.cloudera:8020/sunil/spark/hello-spark")
  }
}
