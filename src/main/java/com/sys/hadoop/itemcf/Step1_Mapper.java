package com.sys.hadoop.itemcf;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 清除数据第一行的无效数据
 * item_id,user_id,action,vtime
 *
 * Create by yang_zzu on 2020/9/1 on 10:33
 */
public class Step1_Mapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        if (key.get() != 0) {
            context.write(value, NullWritable.get());
        }
    }
}
