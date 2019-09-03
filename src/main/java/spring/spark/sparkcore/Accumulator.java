package spring.spark.sparkcore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import scala.Serializable;
import spring.config.UserAccumulatorParam;

import java.util.*;

/**
 * @author ：Cyril
 * @date ：Created in 2019/9/1 17:24
 * @description： spark 累加器, 用来监控调试某类数据特征
 * @modified By：
 */
@Component
public class Accumulator implements Serializable {

    private static Logger logger = LoggerFactory.getLogger(Accumulator.class);
    private StorageLevel storageLevel = new StorageLevel();
    public void accumulator(){
        SparkConf conf = new SparkConf().setAppName("Accumulator").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        /**
         * 注册自定义累加器,否则无法正常使用
         */
        UserAccumulatorParam userAccumulatorParam = new UserAccumulatorParam(0);
        sc.sc().register(userAccumulatorParam,"userAccumulatorParam");
        JavaRDD<Integer> parallelize = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

        JavaRDD<HashMap<Integer, String>> map = parallelize.map(new Function<Integer, HashMap<Integer, String>>() {
            @Override
            public HashMap<Integer, String> call(Integer integer) throws Exception {
                List list = new ArrayList<HashMap<Integer, String>>();
                Map map = new HashMap<Integer, String>();
                System.out.println("integer++++++++++++"+integer);
                if (integer % 2 == 0) {
                    map.put(integer, "偶数");
                    userAccumulatorParam.add(1);
                } else {
                    map.put(integer, "奇数");
                }
                return (HashMap<Integer, String>) map;
            }
        });
        logger.info("第一次打印累加器的值："+userAccumulatorParam.value()); //0
        map.count();
        logger.info("第二次打印累加器的值："+userAccumulatorParam.value()); //5
        map.cache().count();
        logger.info("第三次打印累加器的值："+userAccumulatorParam.value()); //15
        sc.close();
    }
}
