package spark

import org.apache.spark.{SparkConf, SparkContext}

object HelloWorld {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("HelloWorl")

    val sc = new SparkContext(conf)

    val helloWorld = sc.parallelize(List("Hello,World!","Hello,Spark!","Hello,BigData!"))

    helloWorld.foreach(line => println(line))
  }

}
