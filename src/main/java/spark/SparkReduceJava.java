package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

public class SparkReduceJava {

    public static void main(String[] main){

        SparkConf conf = new SparkConf().setAppName("SparkReduceJava").setMaster("local");


        JavaSparkContext sc = new JavaSparkContext(conf);

        reduceJava(sc);

        reduceJava8(sc);
    }


    public static void reduceJava(JavaSparkContext sc){
        JavaRDD<Long>textData = sc.textFile("./avg").map(new Function<String, Long>() {
            @Override
            public Long call(String s) throws Exception {
                return Long.parseLong(s);
            };
        });

        System.out.println(
                textData.reduce(new Function2<Long, Long, Long>() {
                    @Override
                    public Long call(Long aLong, Long aLong2) throws Exception {
                        System.out.println("x:"+aLong);
                        System.out.println("y:"+aLong2);
                        return aLong+aLong2;
                    }
                })/textData.count()
        );
    }

    public static void reduceJava8(JavaSparkContext sc){
        JavaRDD<Long>textData = sc.textFile("./avg").map(s->Long.parseLong(s));
        System.out.println(textData.reduce((x,y)->x+y)/textData.count());
    }

}
