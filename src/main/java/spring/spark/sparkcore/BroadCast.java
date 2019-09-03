package spring.spark.sparkcore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.springframework.stereotype.Component;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author ：Cyril
 * @date ：Created in 2019/9/1 14:59
 * @description： spark广播变量的用法，有时候spark会将闭包中所有的变量分发到各个execute(work)节点去，但是这种方法非常低效，
 *                这时候使用spark中的广播变量可以非常高效的向work发送一个较大的只读值供spark操作使用
 * @modified By：
 */
@Component
public class BroadCast implements scala.Serializable {

    public List<String> broadCast() throws IOException {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("BroadCast");
        JavaSparkContext sc = new JavaSparkContext(conf);
        BufferedReader bufferedReader = new BufferedReader(new FileReader("C:\\Users\\Administrator\\IdeaProjects\\SparkLearn\\src\\main\\resources\\BroadCast.txt"));
        List<String>  broadCastList = new ArrayList<>();
        String str;
        while((str=bufferedReader.readLine())!=null){
            broadCastList.add(str);
        }
        broadCastList.forEach(word-> System.out.println(word));
        Broadcast<List<String>> broadCast = sc.broadcast(broadCastList);
        JavaRDD<String> stringJavaRDD = sc.textFile("C:\\Users\\Administrator\\IdeaProjects\\SparkLearn\\src\\main\\resources\\test.txt");
        JavaRDD<String> filter = stringJavaRDD.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                List<String> value = broadCast.getValue();
                return !value.contains(s);
            }
        });

        List<String> collect = filter.collect();
        collect.forEach(s-> System.out.println(s));
        sc.close();

        /**
         * 记得最后一定要关闭bufferedReader和sc
         */
        if(bufferedReader!=null){
           bufferedReader.close();
        }

        return collect;

    }
}
