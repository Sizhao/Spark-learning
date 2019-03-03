package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

public class SparkIntersectionAndSubtractJava {

    public static void main(String[] args){

        SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkIntersectionAndSubtractJava");

        JavaSparkContext sc = new JavaSparkContext(conf);

        //java7实现
        intersectionAndSubtractJava(sc);

        //java8实现
        intersectionAndSubtractJava8(sc);
    }


    public static void intersectionAndSubtractJava(JavaSparkContext sc){

        JavaRDD<String> lesson1Data = sc.textFile("./lesson1");

        JavaRDD<String> lesson2Data = sc.textFile("./lesson2");

        JavaPairRDD<String,Integer> lesson1InfoData = lesson1Data.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s.split(" ")[0],Integer.parseInt(s.split(" ")[1]));
            }
        });

        JavaPairRDD<String,Integer> lesson2InfoData = lesson2Data.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s.split(" ")[0],Integer.parseInt(s.split(" ")[1]));
            }
        });

        JavaPairRDD<String,Integer> lesson1Grades = lesson1InfoData.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        });

        JavaPairRDD<String,Integer> lesson2Grades = lesson2InfoData.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        });

        JavaRDD<String> lesson1Students = lesson1Grades.map(new Function<Tuple2<String, Integer>, String>() {
            @Override
            public String call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2._1;
            }
        });

        JavaRDD<String> lesson2Students = lesson2Grades.map(new Function<Tuple2<String, Integer>, String>() {
            @Override
            public String call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2._1;
            }
        });

        //既在lesson1中又在lesson2中的学生
        System.out.println("Students On Lesson1 And On Lesson2");
        lesson1Students.intersection(lesson2Students).foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        //既在lesson2中又在lesson1中的学生，与上面的结果一致
        System.out.println("Students On Lesson1 And On Lesson2");
        lesson2Students.intersection(lesson1Students).foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        //只在lesson1中而不在lesson2中的学生
        JavaRDD<String> studensOnlyInLesson1 = lesson1Students.subtract(lesson2Students);
        System.out.println("Students Only In Lesson1");
        lesson1Students.subtract(lesson2Students).foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        //只在lesson2中而不在lesson1中的学生
        JavaRDD<String> studensOnlyInLesson2 = lesson2Students.subtract(lesson1Students);
        System.out.println("Students Only In Lesson2");
        studensOnlyInLesson2.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        //只选了一门课的学生
        JavaRDD<String> onlyOneLesson = studensOnlyInLesson1.union(studensOnlyInLesson2);
        System.out.println("Students Only Choose One Lesson");
        onlyOneLesson.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        System.out.println("All the students");
        lesson1Students.union(lesson2Students).distinct().foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

    }

    public static void intersectionAndSubtractJava8(JavaSparkContext sc){

        JavaRDD<String> lesson1Data = sc.textFile("./lesson1");

        JavaRDD<String> lesson2Data = sc.textFile("./lesson2");


        JavaPairRDD<String,Integer> lesson1InfoData =
        lesson1Data.mapToPair(line -> new Tuple2<>(line.split(" ")[0],Integer.parseInt(line.split(" ")[1])));


        JavaPairRDD<String,Integer> lesson2InfoData =
        lesson2Data.mapToPair(line -> new Tuple2<>(line.split(" ")[0],Integer.parseInt(line.split(" ")[1])));


        JavaPairRDD<String,Integer> lesson1Grades = lesson1InfoData.reduceByKey((x,y) -> x+y);

        JavaPairRDD<String,Integer> lesson2Grades = lesson2InfoData.reduceByKey((x,y) -> x+y);


        JavaRDD<String> studentsInLesson1 = lesson1Grades.map(x->x._1);

        JavaRDD<String> studentsInLesson2 = lesson2Grades.map(x->x._1);

        //既在lesson1中又在lesson2中的学生
        studentsInLesson1.intersection(studentsInLesson2).foreach(name -> System.out.println(name));

        //既在lesson2中又在lesson1中的学生，与上面的结果一致
        studentsInLesson1.intersection(studentsInLesson2).foreach(name -> System.out.println(name));

        //只在lesson1中的学生
        JavaRDD<String> studentsOnlyInLesson1 = studentsInLesson1.subtract(studentsInLesson2);
        studentsOnlyInLesson1.foreach(name -> System.out.println(name));

        //只在lesson2中的学生
        JavaRDD<String> studentsOnlyInLesson2 = studentsInLesson2.subtract(studentsInLesson1);
        studentsOnlyInLesson2.foreach(name -> System.out.println(name));


        //只选了一门课的学生
        JavaRDD<String> studentsOnlyOneLesson = studentsOnlyInLesson1.union(studentsInLesson2);
        studentsOnlyOneLesson.foreach(name -> System.out.println(name));


        studentsInLesson1.union(studentsInLesson2).distinct().foreach(name -> System.out.println(name));


    }

}
