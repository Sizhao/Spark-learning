package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.codehaus.janino.Java;
import scala.Tuple2;
import scala.Tuple3;

public class SparkMapJava {

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkFlatMapJava");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //java实现
        mapJava(sc);


        //java8实现
        mapJava8(sc);


    }

    public static void mapJava(JavaSparkContext sc){
        JavaRDD<String> txtData = sc.textFile("./uv.txt");

        //保留最后一个值
        JavaRDD<String> mapData1 = txtData.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                return s.split(" ")[2];
            }
        });

        System.out.println(mapData1.count());
        System.out.println(mapData1.first());

        //保留最后两个值
        JavaRDD<Tuple2<String,String>> mapData2 = txtData.map(new Function<String, Tuple2<String,String>>() {
            @Override
            public Tuple2<String,String> call(String s) throws Exception {
                return new Tuple2<>(s.split(" ")[1],s.split(" ")[2]);
            }
        });

        System.out.println(mapData2.count());
        System.out.println(mapData2.first());

        //保留最后三个值
        JavaRDD<Tuple3<String,String,String>> mapData3 = txtData.map(new Function<String, Tuple3<String,String,String>>() {
            @Override
            public Tuple3<String,String,String> call(String s) throws Exception {
                return new Tuple3<>(s.split(" ")[0],s.split(" ")[1],s.split(" ")[2]);
            }
        });

        System.out.println(mapData2.count());
        System.out.println(mapData2.first());


    }


    public static void mapJava8(JavaSparkContext sc){
        JavaRDD<String> mapData1 = sc.textFile("./uv.txt").map(line -> line.split(" ")[2]);
        System.out.println(mapData1.count());
        System.out.println(mapData1.first());

        JavaRDD<Tuple2<String,String>> mapData2 = sc.textFile("./uv.txt").map(line -> new Tuple2<String, String>(line.split(" ")[1],line.split(" ")[2]));
        System.out.println(mapData2.count());
        System.out.println(mapData2.first());

        JavaRDD<Tuple3<String,String,String>> mapData3 = sc.textFile("./uv.txt").map(line -> new Tuple3<String, String, String>(line.split(" ")[0],line.split(" ")[1],line.split(" ")[2]));
        System.out.println(mapData3.count());
        System.out.println(mapData3.first());

    }

}
