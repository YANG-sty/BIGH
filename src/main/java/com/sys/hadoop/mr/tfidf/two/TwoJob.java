package com.sys.hadoop.mr.tfidf.two;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Create by yang_zzu on 2020/8/30 on 17:29
 */
public class TwoJob {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration(true);
        Job job = Job.getInstance(configuration);
        job.setJarByClass(TwoJob.class);
        job.setJobName("weibo_2");

        job.setMapperClass(TwoMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setCombinerClass(TwoReduce.class);
        job.setReducerClass(TwoReduce.class);

        FileInputFormat.addInputPath(job, new Path("/data/tfidf/output/weibo_1"));
        Path outputPath = new Path("/data/tfidf/output/weibo_2");
        if (outputPath.getFileSystem(configuration).exists(outputPath)) {
            outputPath.getFileSystem(configuration).delete(outputPath, true);
        }
        FileOutputFormat.setOutputPath(job, outputPath);

        boolean b = job.waitForCompletion(true);
        if (b) {
            System.out.println("第二次执行成功。。。。。。。");
        }


    }
}
