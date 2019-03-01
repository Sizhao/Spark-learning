package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class SparkReduceByKeyJava {

    public static void main(String[] main){

        SparkConf conf = new SparkConf().setAppName("SparkReduceJava").setMaster("local");


        JavaSparkContext sc = new JavaSparkContext(conf);

        reduceByKeyJava(sc);

        reduceByKeyJava8(sc);

    }


    public static void reduceByKeyJava(JavaSparkContext sc){

        JavaPairRDD<Integer,Integer> numData = sc.textFile("./avg").mapToPair(new PairFunction<String, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(String s) throws Exception {
                return new Tuple2<Integer, Integer>(Integer.parseInt(s)%10,1);
            }
        });

        
        System.out.println(numData.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        }).sortByKey().collect());

    }

    public static void reduceByKeyJava8(JavaSparkContext sc){
        JavaPairRDD<Integer,Integer> numData = sc.textFile("./avg").mapToPair(s->new Tuple2<>(Integer.parseInt(s)%10,1));

        System.out.println(numData.reduceByKey((x,y)->x+y).sortByKey().collect());
    }

}
