package spring.spark.sparkcore;

import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author ：Cyril
 * @date ：Created in 2019/8/29 22:44
 * @description： 该函数和map函数类似，只不过映射函数的参数由RDD中的每一个元素变成了RDD中每一个分区的迭代器。
 * 如果在映射的过程中需要频繁创建额外的对象，使用mapPartitions要比map高效的过。
 * 比如，将RDD中的所有数据通过JDBC连接写入数据库，如果使用map函数，可能要为每一个元素都创建一个connection，
 * 这样开销很大，如果使用mapPartitions，那么只需要针对每一个分区建立一个connection。
 * 参数preservesPartitioning表示是否保留父RDD的partitioner分区信息。
 */

@Component
public class MapPartitionos implements Serializable {


    public List<String> run() {

        SparkConf conf = new SparkConf().setAppName("mapPartitions").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> stringJavaRDD = sc.textFile("C:\\Users\\Administrator\\IdeaProjects\\SparkLearn\\src\\main\\resources\\test.txt", 3);


        List<Partition> partitions = stringJavaRDD.partitions();
        for(Partition partition : partitions){
            System.out.println(partition);
        }

        JavaRDD<String> stringJavaRDD1 = stringJavaRDD.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
            @Override
            public Iterator<String> call(Iterator<String> stringIterator) throws Exception {
                List<String> list = new ArrayList<>();
                String line;
                while (stringIterator.hasNext()) {
                    line = stringIterator.next();
                    list.add(line);
                }
                return list.iterator();
            }
        },true);
        List<String> list = stringJavaRDD1.collect();
        sc.close();
        return list;
    }

}
