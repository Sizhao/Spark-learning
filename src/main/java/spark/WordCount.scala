package spark

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("WordCount")

    val sc = new SparkContext(conf)


    //设置数据路径
    val text = sc.textFile("./GoneWithTheWind")

    //将文本数据按行处理，每行按空格拆成一个数组
    // flatMap会将各个数组中元素合成一个大的集合
    val textSplit = text.flatMap(line =>line.split(" "))

    //处理合并后的集合中的元素，每个元素的值为1，返回一个元组（key,value）
    //其中key为单词，value这里是1，即该单词出现一次
    val textSplitFlag = textSplit.map(word => (word,1))

    //reduceByKey会将textSplitFlag中的key相同的放在一起处理
    //传入的（x,y）中，x是上一次统计后的value，y是本次单词中的value，即每一次是x+1
    val countWord = textSplitFlag.reduceByKey((x,y)=>x+y)

    //将计算后的结果存在项目目录下的result目录中
    countWord.saveAsTextFile("./result")

    sc.textFile("./GoneWithTheWind").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).saveAsTextFile("./result1")

    println(textSplit.countByValue())
  }

}
