package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.Arrays;
import java.util.Iterator;

public class SparkFlatMapJava {

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkFlatMapJava");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //java实现
        flatMapJava(sc);


        //java8实现
        flatMapJava8(sc);


    }

    public static void flatMapJava(JavaSparkContext sc){
        //设置数据路径
        JavaRDD<String> textData = sc.textFile("./uv.txt");

        //输出处理前总行数
        System.out.println("before:"+textData.count()+"行");

        //输出处理前第一行数据
        System.out.println("first line:"+textData.first()+"行");

        //进行flatMap处理
        JavaRDD<String> flatData = textData.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        //输出处理后总行数
        System.out.println("after:"+flatData.count()+"行");

        //输出处理后第一行数据
        System.out.println("first line:"+flatData.first()+"行");

        //将结果保存在flatResultScala文件夹中
        flatData.saveAsTextFile("./flatResultJava");
    }


    public static void flatMapJava8(JavaSparkContext sc){
        sc.textFile("./uv.txt")
          .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
          .saveAsTextFile("./flatResultJava8");
    }

}

