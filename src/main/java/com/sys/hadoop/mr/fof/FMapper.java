package com.sys.hadoop.mr.fof;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

/**
 * Create by yang_zzu on 2020/8/26 on 15:33
 */
public class FMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    Text mkey = new Text();
    IntWritable mval = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /**
         * tom hello hadoop cat
         */
        String[] s = StringUtils.split(value.toString(), ' ');

        for (int i = 1; i < s.length; i++) {
            mkey.set(getFof(s[0], s[i]));
            //0 代表的是直接关系
            mval.set(0);
            context.write(mkey, mval);

            for (int i1 = i + 1; i1 < s.length; i1++) {
                mkey.set(getFof(s[i], s[i1]));
                //1 代表的是间接关系
                mval.set(1);
                context.write(mkey, mval);
            }
        }

    }

    /**
     * 对两个字符串进行排序处理
     * @param s1
     * @param s2
     * @return
     */
    public static String getFof(String s1, String s2) {
        if (s1.compareTo(s2) < 0) {
            return s1 + ":" + s2;
        }
        return s2 + ":" + s1;
    }



}
