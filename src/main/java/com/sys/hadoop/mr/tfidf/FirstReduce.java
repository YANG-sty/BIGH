package com.sys.hadoop.mr.tfidf;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Create by yang_zzu on 2020/8/29 on 16:49
 */
public class FirstReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0;
        for (IntWritable value : values) {
            sum = sum + value.get();
        }
        if (key.equals(new Text("count"))) {
            System.out.println(key.toString() + "_________________" + sum);
        }
        context.write(key, new IntWritable(sum));

    }
}
