package com.sys.hadoop.mr.tq;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Create by yang_zzu on 2020/8/25 on 19:23
 *
 * Reducer<TQ, IntWritable, Text, IntWritable>
 *     TQ 表示 Reduce 的输入key，====》 mapper 输出 key
 *     IntWritable 表示 Reduce 的输入 value ===> mapper 输出的 value
 *
 *     Text 表示 Reduce 的输出 key
 *     IntWritable 表示 Reduce 的输出 value
 */
public class TReducer extends Reducer<TQ, IntWritable, Text, IntWritable> {


    Text rkey = new Text();
    IntWritable rval = new IntWritable();

    @Override
    protected void reduce(TQ key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        /**
         * key:  1949 10 01 88
         * value: 88
         *
         * 1949 10 01 88
         * 1949 10 02 78
         * 1949 10 04 68
         * 1949 10 05 58
         *
         * 相同的 key 为一组
         */
        int flag = 0;
        int day = 0;
        for (IntWritable value : values) {
            if (flag == 0) {
                String s = key.getYear() + "-" + key.getMonth() + "-" + key.getDay() + "-" + key.getWd();
                //输出key
                rkey.set(s);
                //输出 value
                rval.set(Integer.valueOf(String.valueOf(value)));
                flag++;
                day = key.getDay();
                //输出
                context.write(rkey,rval);
            }
            if (flag != 0 && day != key.getDay()) {
                String s = key.getYear() + "-" + key.getMonth() + "-" + key.getDay() + "-" + key.getWd();
                //输出key
                rkey.set(s);
                //输出 value
                rval.set(Integer.valueOf(String.valueOf(value)));
                //输出
                context.write(rkey,rval);

                //这个if 获得的是该月中第二高的温度,获得之后，后面有再多的数据也不会进行处理
                break;
            }

        }




    }
}
