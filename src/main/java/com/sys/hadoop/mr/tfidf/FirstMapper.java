package com.sys.hadoop.mr.tfidf;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;

/**
 * Create by yang_zzu on 2020/8/29 on 16:32
 */
public class FirstMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //3823890210294392	今天我约了豆浆，油条
        String[] split = value.toString().trim().split("\t");
        if (split.length >= 2) {
            String id = split[0].trim();
            String content = split[1].trim();

            StringReader sr = new StringReader(content);
            IKSegmenter ikSegmenter = new IKSegmenter(sr, true);
            Lexeme word = null;
            while ((word = ikSegmenter.next()) != null) {
                String lexemeText = word.getLexemeText();
                //今天_3823890210294392	1
                context.write(new Text(lexemeText + "_" + id), new IntWritable(1));
            }
            //count 1
            context.write(new Text("count"), new IntWritable(1));

        } else {
            System.out.println(value.toString() + "--------------------");

        }
    }
}
