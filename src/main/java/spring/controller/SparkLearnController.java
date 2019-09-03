package spring.controller;



import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.rdd.RDD;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import scala.Tuple2;
import spring.spark.sparkcore.*;

import java.beans.Transient;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

@Component
@RestController
public class SparkLearnController implements Runnable{

    @Autowired
    private SparkLearnController sparkLearnController;

    @Autowired
    private MapPartitionos mapPartitionos;

    @Autowired
    private WordCount wc;

    @Autowired
    private MapPartitionsWithIndex mapPartitionsWithIndex;

    @Autowired
    private BroadCast broadCast;

    @Autowired
    private Accumulator accumulator;

    @Autowired
    private GroupByKey groupByKey;

    @Autowired
    private AggregateByKey aggregateByKey;

    @Autowired
    private CoGroup coGroup;

    @Autowired
    private Reduce reduce;



    @RequestMapping("/reduce")
    public String reduce(){
        int num = this.reduce.reduce();
        String str = "结果："+num;
        System.out.println(str);
        return str;
    }


    @RequestMapping("/coGroup")
    public String coGroup(){
        List<Tuple2<String, Tuple2<Iterable<Integer>, Iterable<Integer>>>> list = this.coGroup.coGroup();
        System.out.println(list);
        return list.toString();
    }



    @RequestMapping("/hello")
    public String getWord(){
        System.out.println(this.sparkLearnController);
        return "Hello World";
    }



    @RequestMapping("/wordCount")
    public void test(){
        this.wc.wordCount();
    }


    @Override
    public void run() {
        for(int i=0;i<10000;i++){
            System.out.println("i:"+i);
            try {
                Thread.currentThread().sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @RequestMapping("/mapPartitions")
    public String mapPartitions(){
        List<String> list = this.mapPartitionos.run();
        StringBuilder builder = new StringBuilder();
        for(String str:list){
            builder=builder.append(str+"  \n");
        }
        System.out.println(builder);
        return builder.toString();
    }


    @RequestMapping("/mapPartitionsWithIndex/1")
    public String mapPartitionsWithIndex_1(){
        List list = this.mapPartitionsWithIndex.mapPartitionsWithIndex_1();
        StringBuilder stringBuilder = new StringBuilder();
        list.forEach(rdd-> System.out.println(rdd));
        System.out.println(this.getClass()+" " + list);
        for(Object obj:list){
            stringBuilder= stringBuilder.append(obj.toString()+" \n");
        }
        return stringBuilder.toString();
    }

    @RequestMapping("/mapPartitionsWithIndex/2")
    public String mapPartitionsWithIndex_2(){
        List list = this.mapPartitionsWithIndex.mapPartitionsWithIndex_2();
        StringBuilder stringBuilder = new StringBuilder();
        list.forEach(rdd-> System.out.println(rdd));
        for(Object obj:list){
            Tuple2<String,Integer> tuple2 = (Tuple2<String,Integer>) obj;
            stringBuilder=stringBuilder.append(tuple2._1+"--->"+tuple2._2);
        }
        return stringBuilder.toString();
    }

    @RequestMapping("/mapPartitionsWithIndex/3")
    public String mapPartitionsWithIndex_3(){
        List list = this.mapPartitionsWithIndex.mapPartitionsWithIndex_3();
        StringBuilder stringBuilder = new StringBuilder();
        list.forEach(rdd-> System.out.println(rdd));
        for(Object obj:list){
            Tuple2<String,Integer> tuple2 = (Tuple2<String,Integer>) obj;
            stringBuilder=stringBuilder.append(tuple2._1+"--->"+tuple2._2);
        }
        return stringBuilder.toString();
    }


    @RequestMapping("/broadCast")
    public String broadCast() throws IOException {
        List<String> stringList = this.broadCast.broadCast();
        StringBuilder stringBuilder = new StringBuilder();
        for(String str:stringList){
            stringBuilder = stringBuilder.append(str + " \n");
        }
        return stringBuilder.toString();
    }


    @RequestMapping("/accumulator")
    public void accumulator(){
        this.accumulator.accumulator();
    }

    @RequestMapping("/groupByKey")
    public String groupByKey(){
        List<Tuple2<String, Iterable<Integer>>> tuple2s = this.groupByKey.groupByKey();
        System.out.println(tuple2s);
        return tuple2s.toString();
    }


    @RequestMapping("/aggregateByKey")
    public String aggregateByKey(){
        List<Tuple2<String, Integer>> list = this.aggregateByKey.aggregateByKey();
        System.out.println(list);
        return list.toString();
    }
}
