package spring.spark.sparkcore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author ：Cyril
 * @date ：Created in 2019/8/31 13:56
 * @description： MapPartitionsWithIndex算子：用法上和MapPartitions大同小异，只是加上一个INDEX值表示分区信息,将一个分区的所有元素进行同样的操作
 * @modified By：
 */

@Component
public class MapPartitionsWithIndex implements Serializable {

    public List<String> mapPartitionsWithIndex_1(){
        SparkConf conf = new SparkConf().setAppName("mapPartitionsWithIndex").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> javaRDD = sc.parallelize(Arrays.asList(1,2,3,4,5,6,7,8,9),3);
        JavaRDD<String> stringJavaRDD = javaRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer integer, Iterator<Integer> integerIterator) throws Exception {
                List<String> rddWithIndex = new ArrayList<>();
                String rdd = null;
                while (integerIterator.hasNext()) {
                    rdd = "partition:" + integer + "--->" + integerIterator.next();
                    rddWithIndex.add(rdd);
                }
                return rddWithIndex.iterator();
            }
        }, true);
        List<String> collect = stringJavaRDD.collect();
        sc.close();
        System.out.println(collect);
        return collect;
    }


    /**
     * 对每个分区内的数字求和打印  mapPartitionsWithIndex_2() 毫无意义
     * @return
     */
    public List<Tuple2<String,Integer>> mapPartitionsWithIndex_2(){
        SparkConf conf = new SparkConf().setAppName("mapPartitionsWithIndex").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> javaRDD = sc.parallelize(Arrays.asList(1,2,3,4,5,6,7,8,9),3);
        JavaRDD<Tuple2<String, Integer>> tuple2JavaRDD = javaRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<Tuple2<String, Integer>>>() {
            @Override
            public Iterator<Tuple2<String, Integer>> call(Integer integer, Iterator<Integer> integerIterator) throws Exception {
                List<Tuple2<String, Integer>> list = new ArrayList<>();
                while (integerIterator.hasNext()) {
                    list.add(new Tuple2<>("partition:" + integer, integerIterator.next()));
                }
                return list.iterator();
            }
        }, true);

        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD = tuple2JavaRDD.mapToPair(stringIntegerTuple2 -> new Tuple2<String,Integer>(stringIntegerTuple2._1,stringIntegerTuple2._2));
        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD1 = stringIntegerJavaPairRDD.reduceByKey((integer1, integer2) -> integer1 + integer2);
        List<Tuple2<String, Integer>> collect = stringIntegerJavaPairRDD1.collect();
        sc.close();
        return collect;
    }


    /**
     * 对每个分区内的数字求和打印
     * 改进mapPartitionsWithIndex_3():测试是否可以通过分区直接加,不用转换成JavaPairRDD 然后通过算子reduceByKey()才行
     * @return
     */
    public List<Tuple2<String,Integer>> mapPartitionsWithIndex_3(){
        SparkConf conf = new SparkConf().setAppName("mapPartitionsWithIndex").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> javaRDD = sc.parallelize(Arrays.asList(1,2,3,4,5,6,7,8,9),3);
        /**
         * 可以更好的理解mapPartitionsWithIndex()是针对一个分区进行的操作,将同一个分区下的数字相加,返回一个新的RDD,总共返回三个新的RDD;
         */
        JavaRDD<Tuple2<String, Integer>> tuple2JavaRDD = javaRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<Tuple2<String, Integer>>>() {
            @Override
            public Iterator<Tuple2<String, Integer>> call(Integer integer, Iterator<Integer> integerIterator) throws Exception {
                List<Tuple2<String, Integer>> list = new ArrayList<>();
                int i = 0;
                while (integerIterator.hasNext()) {
                    i += integerIterator.next();
                }
                list.add(new Tuple2<>("partition:" + integer, i));
                return list.iterator();
            }
        }, true);

        /**
         * 可以避免以下操作,无意义
         */
//        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD = tuple2JavaRDD.mapToPair(stringIntegerTuple2 -> new Tuple2<String,Integer>(stringIntegerTuple2._1,stringIntegerTuple2._2));
//        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD1 = stringIntegerJavaPairRDD.reduceByKey((integer1, integer2) -> integer1 + integer2);
//        List<Tuple2<String, Integer>> collect = stringIntegerJavaPairRDD1.collect();
        List<Tuple2<String, Integer>> collect = tuple2JavaRDD.collect();
        sc.close();
        return collect;
    }



}
