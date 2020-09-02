package com.sys.hadoop.itemcf;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Create by yang_zzu on 2020/9/1 on 10:44
 */
public class Step2_Mapper extends Mapper<LongWritable, Text, Text, Text> {

    Text rkey = new Text();
    Integer rv;
    Text rValue = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //i161,u2625,click,2014/9/18 15:03
        String[] splits = value.toString().split(",");
        String item = splits[0];
        String user = splits[1];
        String action = splits[2];

        rkey.set(String.valueOf(user));
        rv = StartRun.R.get(action);
        rValue.set(String.valueOf(item + ":" + rv.intValue()));

        /**
         * u2625    i161:1
         */
        context.write(rkey, rValue);

    }
}
