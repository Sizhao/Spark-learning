package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class SparkMapToPair {

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkFlatMapJava");
        JavaSparkContext sc = new JavaSparkContext(conf);

        mapToPairJava(sc);

        mapToPairJava8(sc);

    }


    public static void mapToPairJava(JavaSparkContext sc){

        JavaPairRDD<String,String> pairRDD = sc.textFile("./uv.txt").mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                return new Tuple2<>(s.split(" ")[1],s.split(" ")[2]);
            }
        });

        System.out.println(pairRDD.count());

        System.out.println(pairRDD.first());

    }

    public static void mapToPairJava8(JavaSparkContext sc){
        JavaPairRDD<String,String> pairRDD = sc.textFile("./uv.txt").mapToPair(line -> new Tuple2<>(line.split(" ")[1],line.split(" ")[2]));

        System.out.println(pairRDD.count());

        System.out.println(pairRDD.first());
    }
}
