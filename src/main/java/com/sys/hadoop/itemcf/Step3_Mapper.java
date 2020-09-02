package com.sys.hadoop.itemcf;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Create by yang_zzu on 2020/9/1 on 11:20
 */
public class Step3_Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text mkey = new Text();
    private IntWritable mvalue = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //u3244	i469:1,i498:1,i154:1,i73:1,i162:1,
        String[] split = value.toString().split("\t");
        String[] items = split[1].split(",");
        //将用户的 物品进行两两相与的操作
        for (int i = 0; i < items.length; i++) {

            String itemA = items[i].split(":")[0];

            for (int j = 0; j < items.length; j++) {
                String itemB = items[j].split(":")[0];
                mkey.set(itemA + ":" + itemB);

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
                context.write(mkey, mvalue);
            }
        }
    }
}
