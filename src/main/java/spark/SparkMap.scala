package spark

import org.apache.spark.{SparkConf, SparkContext}

object SparkMap {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("SparkFlatMap")

    val sc = new SparkContext(conf)

    val textData = sc.textFile("./uv.txt")

    //得到一个最后一个操作值，前面的时间和次数舍弃
    val mapData1 = textData.map(line => line.split(" ")(2))

    println(mapData1.count())

    println(mapData1.first())

    mapData1.saveAsTextFile("./resultMapScala")

    //得到一个最后两个值，前面的时间舍弃
    val mapData2 = textData.map(line => (line.split(" ")(1),line.split(" ")(2)))

    println(mapData2.count())

    println(mapData2.first())

    //将所有值存到元组中去
    val mapData3 = textData.map(line => (line.split(" ")(1),line.split(" ")(1),line.split(" ")(2)))

    println(mapData3.count())

    println(mapData3.first())



  }

}
