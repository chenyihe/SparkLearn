package spring.spark.sparkcore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @author ：Cyril
 * @date ：Created in 2019/8/31 12:49
 * @description： spark测试用例Word Count
 * @modified By：
 */

@Component
public class WordCount {
    public void wordCount(){
        SparkConf conf = new SparkConf().setAppName("wordCount").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> stringJavaRDD = sc.textFile("C:\\Users\\Administrator\\IdeaProjects\\SparkLearn\\src\\main\\resources\\test.txt");
        JavaRDD<Tuple2<String,Integer>> map = stringJavaRDD.map(s -> new Tuple2(s, s.hashCode()));
        long count = map.count();
        System.out.println("count:" + count);
        /**
         * mapPartitions 将javardd中的所有rdd当成一个集合，实现FlatMapFunction对这个集合中的每个元素就行操作，看作一个整体
         */
        JavaRDD<String> stringJavaRDD1 = stringJavaRDD.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD = stringJavaRDD1.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD1 = stringIntegerJavaPairRDD.reduceByKey((integer1, integer2) -> integer1 + integer2);
        JavaPairRDD<Integer, String> integerStringJavaPairRDD = stringIntegerJavaPairRDD1.mapToPair(s -> new Tuple2<>(s._2, s._1));
        JavaPairRDD<Integer, String> integerStringJavaPairRDD1 = integerStringJavaPairRDD.sortByKey();
        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD2 = integerStringJavaPairRDD1.mapToPair(s -> new Tuple2<>(s._2, s._1));
        List<Tuple2<String, Integer>> collect = stringIntegerJavaPairRDD2.collect();
        for(Tuple2<String,Integer> tuple2:collect){
            System.out.println("key:"+tuple2._1+" "+"value:"+tuple2._2);
        }
    }
}
