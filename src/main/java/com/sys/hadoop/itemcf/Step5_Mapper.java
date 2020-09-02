package com.sys.hadoop.itemcf;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 * Create by yang_zzu on 2020/9/1 on 15:37
 */
public class Step5_Mapper extends Mapper<LongWritable, Text, Text, Text> {
    Text mkey = new Text();
    Text mvalue = new Text();

    /**
     * 不进行操作，拿到什么，输出什么
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] split = Pattern.compile("[\t,]").split(value.toString());
        mkey.set(split[0]);
        mvalue.set(split[1] + "," + split[2]);

        context.write(mkey, mvalue);

    }
}
