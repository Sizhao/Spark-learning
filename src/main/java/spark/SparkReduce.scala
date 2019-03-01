package spark

import org.apache.spark.{SparkConf, SparkContext}

object SparkReduce {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("SparkReduce")

    val sc = new SparkContext(conf)

    //将String转成Long类型
    val numData = sc.textFile("./avg").map(num => num.toLong)

    //reduce处理每个值
    println("平均值是:"+numData.reduce((x,y)=>{
      println("x:"+x)
      println("y:"+y)
      x+y
    })/numData.count())

  }

}
