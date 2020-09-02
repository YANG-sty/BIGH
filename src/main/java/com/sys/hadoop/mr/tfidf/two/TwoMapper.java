package com.sys.hadoop.mr.tfidf.two;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Create by yang_zzu on 2020/8/30 on 17:36
 */
public class TwoMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 获取当前 mapper task的数据片段（split）
        FileSplit fileSplit = (FileSplit) context.getInputSplit();

        if (!fileSplit.getPath().getName().contains("part-r-00003")) {
            //豆浆_3823890201582094	3
            String[] split = value.toString().trim().split("\t");
            if (split.length >= 2) {
                String[] s = split[0].split("_");
                if (s.length >= 2) {
                    String w = s[0];
                    context.write(new Text(w), new IntWritable(1));
                }
            } else {
                System.out.println(value.toString() + "------------------");
            }
        }


    }
}
