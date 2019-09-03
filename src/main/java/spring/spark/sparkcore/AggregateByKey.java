package spring.spark.sparkcore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.springframework.stereotype.Component;
import scala.Serializable;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @author ：Cyril
 * @date ：Created in 2019/9/3 22:06
 * @description： 类似reduceByKey，对pairRDD中想用的key值进行聚合操作，使用初始值（seqOp中使用，而combOpenCL中未使用）对应返回值为pairRDD，而区于aggregate（返回值为非RDD）
 * @modified By：
 */
@Component
public class AggregateByKey implements Serializable {

    public List<Tuple2<String, Integer>> aggregateByKey(){
        SparkConf conf = new SparkConf().setAppName("AggregateByKey").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> parallelize = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD = parallelize.mapToPair(new PairFunction<Integer, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Integer integer) throws Exception {
                Tuple2<String, Integer> tuple2;
                if (integer % 2 == 0) {
                    tuple2 = new Tuple2<>("偶数", integer);
                } else {
                    tuple2 = new Tuple2<>("奇数", integer);
                }
                return tuple2;
            }
        });
        /**
         * new Function2<Integer,Integer,Integer> 三个参数分别表示 1：初始值 2：rdd原来的value类型 3：返回类型
         */
        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD1 = stringIntegerJavaPairRDD.aggregateByKey(3, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        }, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer*2 + integer2;
            }
        });
        List<Tuple2<String, Integer>> collect = stringIntegerJavaPairRDD1.collect();


        sc.close();
        return collect;

    }




}
