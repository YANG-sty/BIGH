package com.sys.hadoop.mr.fof;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Create by yang_zzu on 2020/8/26 on 15:41
 */
public class FReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    IntWritable rval = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        /**
         * hello:hadoop 0
         * hello:hadoop 1
         * hello:hadoop 0
         */
        int flg = 0;
        int sum = 0;
        for (IntWritable value : values) {
            if (value.get() == 0) {
                flg = 1;
            }
            //sum 表示间接好友数量
            sum += value.get();
        }

        //在循环结束后如果 flg 的值仍然为 0 ,则说明，没有直接关系
        if (flg == 0) {
            rval.set(sum);
            context.write(key, rval);
        }

    }
}
