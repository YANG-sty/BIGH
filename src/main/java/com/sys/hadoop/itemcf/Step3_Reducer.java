package com.sys.hadoop.itemcf;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Create by yang_zzu on 2020/9/1 on 11:36
 */
public class Step3_Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable rvalue = new IntWritable(1);

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        /**
         * i469:i469    1
         * i469:i498    1
         * i469:i154    1
         * i469:i73     1
         * i469:i162    1
         *
         * i498:i469    1
         * i498:i498    1
         * i498:i154    1
         * i498:i73     1
         * i498:i162    1
         */

        int sum = 0;
        for (IntWritable value : values) {
            sum = sum + value.get();
        }
        rvalue.set(sum);

        /**
         * i469:i469    1
         * i469:i498    2
         * i469:i154    2
         * i469:i73     3
         * i469:i162    1
         */
        context.write(key, rvalue);
    }
}
