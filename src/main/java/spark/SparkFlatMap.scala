package spark

import org.apache.spark.{SparkConf, SparkContext}

object SparkFlatMap {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("SparkFlatMap")

    val sc = new SparkContext(conf)

    //设置数据路径
    val textData = sc.textFile("./uv.txt")

    //输出处理前总行数
    println("before:"+textData.count()+"行")

    //输出处理前第一行数据
    println("first line:"+textData.first())

    //进行flatMap处理
    val flatData = textData.flatMap(line => line.split(" "))

    //输出处理后总行数
    println("after:"+flatData.count())

    //输出处理后第一行数据
    println("first line:"+flatData.first())

    //将结果保存在flatResultScala文件夹中
    flatData.saveAsTextFile("./flatResultScala")

  }

}
