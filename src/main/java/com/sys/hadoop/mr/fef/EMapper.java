package com.sys.hadoop.mr.fef;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

/**
 * Create by yang_zzu on 2020/8/26 on 20:11
 */
public class EMapper extends Mapper<LongWritable, Text, FE, IntWritable> {

    FE mkey = new FE();
    IntWritable mval = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /**
         * value
         * cat:hadoop      2
         */
        String[] split = StringUtils.split(value.toString(), '\t');
        String[] users = split[0].split(":");
        mkey.setUser(users[0]);
        mkey.setFriend(users[1]);
        mkey.setQuantity(Integer.parseInt(split[1]));

        mval.set(Integer.parseInt(split[1]));

        //输出  cat:hadoop 2
        context.write(mkey, mval);

        mkey.setUser(users[1]);
        mkey.setFriend(users[0]);
        //输出  hadoop:cat 2
        context.write(mkey, mval);

    }
}
