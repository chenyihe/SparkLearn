package spring.spark.sparkcore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.springframework.stereotype.Component;
import scala.Serializable;

import java.util.Arrays;

/**
 * @author ：Cyril
 * @date ：Created in 2019/9/4 0:03
 * @description： reduce将RDD中元素两两传递给输入函数，同时产生一个新值，新值与RDD中下一个元素再被传递给输入函数，直到最后只有一个值为止。
 * @modified By：
 */
@Component
public class Reduce implements Serializable {

    public Integer reduce(){
        SparkConf conf = new SparkConf().setAppName("Reduce").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> parallelize = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        /**
         *当setMaster("local[2]")里面设置的线程数量不同时，会有很大的差异，当多个线程时，最后是将每个线程reduce得到的数当作在进行reduce
         */
        Integer reduce = parallelize.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                System.out.println(integer+"*10+"+integer2);
                return integer*10 + integer2;
            }
        });

        return reduce;
    }

}
