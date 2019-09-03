package spring.config;

import org.apache.spark.AccumulatorParam;
import org.apache.spark.AccumulatorParam$class;
import org.apache.spark.util.AccumulatorV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Serializable;

/**
 * @author ：Cyril
 * @date ：Created in 2019/9/1 17:32
 * @description： 用户自定义累加器
 * @modified By：
 */

public class UserAccumulatorParam extends AccumulatorV2<Integer,String> implements Serializable {

    private static Logger logger = LoggerFactory.getLogger(UserAccumulatorParam.class);

    private Integer num;


    @Override
    public boolean isZero() {
        return this.num == 0;
    }

    @Override
    public AccumulatorV2 copy() {
        return new UserAccumulatorParam(this.num);
    }

    @Override
    public void reset() {
        this.num = 0;
    }

    @Override
    public synchronized void add(Integer integer) {
        this.num = this.num + integer;
    }


    @Override
    public void merge(AccumulatorV2 accumulatorV2) {
        if(!(accumulatorV2 instanceof UserAccumulatorParam)) {
            logger.info("Debug: Accumulator class is different!");
            return;
        }
        this.num += this.num + ((UserAccumulatorParam) accumulatorV2).num;
    }

    @Override
    public String value() {
        return "累加器的值："+this.num;
    }

    public UserAccumulatorParam(Integer num){
        this.num = num;
    }
}
