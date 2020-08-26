package com.sys.hadoop.mr.tq;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Create by yang_zzu on 2020/8/25 on 17:07
 *
 * Mapper<LongWritable, Text, TQ, IntWritable>
 *     LongWritable 输入的key, 行的编号
 *     Text 输入的value, 读取的一行的数据
 *
 *     TQ mapper 输出的key
 *     IntWritable mapper 输出的value
 */
public class TMapper extends Mapper<LongWritable, Text, TQ, IntWritable> {

    TQ mkey = new TQ();
    IntWritable mval = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //value:  1949-10-01 14:21:02	34c  >>  TQ

        try {
            String[] split = StringUtils.split(value.toString(), '\t');

            //获得日期
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            Date parse = simpleDateFormat.parse(split[0]);
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(parse);
            mkey.setYear(calendar.get(Calendar.YEAR));
            mkey.setMonth(calendar.get(Calendar.MONTH) + 1);
            mkey.setDay(calendar.get(Calendar.DAY_OF_MONTH));
            //获取温度
            String substring = split[1].substring(0, split[1].length() - 1);
            int wd = Integer.valueOf(substring);
            mkey.setWd(wd);

            //设置value
            mval.set(wd);

            //输出到文件
            context.write(mkey, mval);
        } catch (ParseException e) {
            e.printStackTrace();
        }

    }


}
