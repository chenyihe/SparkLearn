package spring.spark.sparkcore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.springframework.stereotype.Component;
import scala.Serializable;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @author ：Cyril
 * @date ：Created in 2019/9/3 20:13
 * @description： Transformation算子：在一个PairRDD或（k,v）RDD上调用，返回一个（k,Iterable<v>）。
 * 主要作用是将相同的所有的键值对分组到一个集合序列当中，其顺序是不确定的。groupByKey是把所有的键值对集合都加载到内存中存储计算，若一个键对应值太多，则易导致内存溢出。慎用
 * @modified By：
 */

@Component
public class GroupByKey implements Serializable {

    public List<Tuple2<String, Iterable<Integer>>> groupByKey(){
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("GroupByKey");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> parallelize = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12));
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

        JavaPairRDD<String, Iterable<Integer>> stringIterableJavaPairRDD = stringIntegerJavaPairRDD.groupByKey();
        List<Tuple2<String, Iterable<Integer>>> collect = stringIterableJavaPairRDD.collect();
        return collect;
    }


}
