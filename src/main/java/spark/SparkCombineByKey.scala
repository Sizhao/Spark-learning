package spark

import org.apache.spark.{SparkConf, SparkContext}

object SparkCombineByKey {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("SparkCombineByKey")

    val sc = new SparkContext(conf)

    sc.textFile("./grades").map(line=>{
      val splits = line.split(" ")
      (splits(0),splits(1).toInt)
    }).combineByKey(
      value => {
        println("这是第一个函数")
        println("将所有的值遍历，并放在元组中，标记1")
        println(value)
        (value,1)
      },
      (x:(Int,Int),y)=>{
        println("这是第二个函数")
        println("将x中的第一个值进行累加求和，第二个值加一，求得元素总个数")
        println("x:"+x.toString())
        println("y:"+y)
        (x._1+y,x._2+1)
      },
      (x:(Int,Int),y:(Int,Int))=>{
        (x._1+y._1,x._2+y._2)
      }
    ).map(x=>(x._1,x._2._1/x._2._2)).foreach(println)

  }

}
