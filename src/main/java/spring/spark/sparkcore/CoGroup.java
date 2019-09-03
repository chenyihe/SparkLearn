package spring.spark.sparkcore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * @author ：Cyril
 * @date ：Created in 2019/9/3 22:33
 * @description： 合并两个RDD，生成一个新的RDD。实例中包含两个Iterable值，第一个表示RDD1中相同值，第二个表示RDD2中相同值（key值），
 * 这个操作需要通过partitioner进行重新分区，因此需要执行一次shuffle操作。（若两个RDD在此之前进行过shuffle，则不需要）
 * @modified By：
 */
@Component
public class CoGroup implements Serializable {

    public List<Tuple2<String, Tuple2<Iterable<Integer>, Iterable<Integer>>>> coGroup(){
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("CoGroup");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> parallelize = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        JavaRDD<Integer> parallelize1 = sc.parallelize(Arrays.asList(6,7,8,9,10,11,12,13,14,15,16));

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

        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD1 = parallelize1.mapToPair(new PairFunction<Integer, String, Integer>() {
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

        JavaPairRDD<String, Tuple2<Iterable<Integer>, Iterable<Integer>>> cogroup = stringIntegerJavaPairRDD1.cogroup(stringIntegerJavaPairRDD);

        //[(偶数,([6, 8, 10, 12, 14, 16],[2, 4, 6, 8, 10])), (奇数,([7, 9, 11, 13, 15],[1, 3, 5, 7, 9]))]
        //注意：两个iterable是指具有相同key的值，值不一定相等
        List<Tuple2<String, Tuple2<Iterable<Integer>, Iterable<Integer>>>> collect = cogroup.collect();
        return collect;


    }

}
