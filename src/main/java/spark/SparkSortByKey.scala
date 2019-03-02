package spark

import org.apache.spark.{SparkConf, SparkContext}

object SparkSortByKey {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("SparkCombineByKey")

    val sc = new SparkContext(conf)


    val result = sc.textFile("./grades").map(line=>{
      val splits = line.split(" ")
      (splits(0),splits(1).toInt)
    }).combineByKey(value =>(value,1),(x:(Int,Int),y)=>(x._1+y,x._2+1),(x:(Int,Int),y:(Int,Int))=>(x._1+y._1,x._2+y._2)
    ).map(x=>(x._1,x._2._1/x._2._2))

    //按照名字排序，顺序
    result.sortByKey(true).foreach(println)

    //按照名字排序，倒序
    result.sortByKey(false).foreach(println)


    val result1 = sc.textFile("./grades").map(line=>{
      val splits = line.split(" ")
      (splits(0),splits(1).toInt)
    }).combineByKey(value =>(value,1),(x:(Int,Int),y)=>(x._1+y,x._2+1),(x:(Int,Int),y:(Int,Int))=>(x._1+y._1,x._2+y._2)
    ).map(x=>(x._2._1/x._2._2,x._1))

    //按照成绩排序，顺序
    result1.sortByKey(true).foreach(println)

    //按照成绩排序，倒序
    result1.sortByKey(false).foreach(println)


  }

}
