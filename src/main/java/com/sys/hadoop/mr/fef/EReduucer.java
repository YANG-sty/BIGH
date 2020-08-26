package com.sys.hadoop.mr.fef;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Create by yang_zzu on 2020/8/26 on 20:26
 */
public class EReduucer extends Reducer<FE, IntWritable, Text, IntWritable> {

    Text rkey = new Text();
    IntWritable rval = new IntWritable();

    @Override
    protected void reduce(FE key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        /**
         * hello hadoop 3 3
         * hello hiv 2 2
         * hello tomcat 1 1
         */
        for (IntWritable value : values) {
            String s = key.getUser() + "-" + key.getFriend();
            rkey.set(s);

            //输出
            context.write(rkey, value);
        }

    }
}
