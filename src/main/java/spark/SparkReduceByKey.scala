package spark

import org.apache.spark.{SparkConf, SparkContext}

object SparkReduceByKey {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("SparkReduce")

    val sc = new SparkContext(conf)

    //将String转成Long类型
    val numData = sc.textFile("./avg").map(num => (num.toLong%10,1))


    //根据key排序后输出
    numData.reduceByKey((x,y)=>x+y).sortByKey().foreach(println(_))
  }

}
