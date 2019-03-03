package spark

import org.apache.spark.{SparkConf, SparkContext}

object SparkIntersectionAndSubtract {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("SparkIntersectionAndSubtract")

    val sc = new SparkContext(conf)

    //课程一中的数据
    val lesson1Data = sc.textFile("./lesson1").map(line => (line.split(" ")(0),line.split(" ")(1).toInt))

    //将课程一中每个人的分数相加
    val lesson1Grade = lesson1Data.reduceByKey(_+_)

    val lesson1Student = lesson1Grade.map(x=>x._1)

    //课程二中的数据处理
    val lesson2Data = sc.textFile("./lesson2").map(line => (line.split(" ")(0),line.split(" ")(1).toInt))

    //将课程二中每个人的分数相加
    val lesson2Grade = lesson2Data.reduceByKey((x,y)=>x+y)

    val lesson2Student = lesson2Grade.map(x=>x._1)

    //在课程一中的人且在课程二中的人的集合
    println("Students On Lesson1 And On Lesson2")
    lesson1Student.intersection(lesson2Student).foreach(println)

    //在课程二中的人且在课程一中的人的集合，与上面的结果一致
    println("Students On Lesson1 And On Lesson2")
    lesson2Student.intersection(lesson1Student).foreach(println)

    //在课程一中的人但不在课程二中的人的集合
    println("Students Only In Lesson1")
    val onlyInLesson1 = lesson1Student.subtract(lesson2Student)
    onlyInLesson1.foreach(println)

    //在课程二中的人但不在课程二中的人的集合
    println("Students Only In Lesson2")
    val onlyInLesson2 = lesson2Student.subtract(lesson1Student)
    onlyInLesson2.foreach(println)


    //只选了一门课的同学
    println("Students Only Choose One Lesson")
    lesson1Student.union(lesson2Student).foreach(println)

    //两门课所有学生（不重复）
    println("All the students")
    lesson1Student.union(lesson2Student).distinct().foreach(print)



  }

}
