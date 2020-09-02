package com.sys.hadoop.mr.tfidf.first;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 第一个MR，计算TF和计算N(微博总数)
 *
 * Create by yang_zzu on 2020/8/29 on 16:22
 */
public class FirstJob {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration(true);

        Job job = Job.getInstance(configuration);
        job.setJarByClass(FirstJob.class);
        job.setJobName("weibo_1");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(FirstMapper.class);

        job.setPartitionerClass(FirstPartition.class);
        job.setCombinerClass(FirstReduce.class);
        job.setReducerClass(FirstReduce.class);
        job.setNumReduceTasks(4);

        Path inputPath = new Path("/data/tfidf/input/");
        FileInputFormat.addInputPath(job, inputPath);

        Path outputPath = new Path("/data/tfidf/output/weibo_1");
        if (outputPath.getFileSystem(configuration).exists(outputPath)) {
            outputPath.getFileSystem(configuration).delete(outputPath, true);
        }
        FileOutputFormat.setOutputPath(job, outputPath);

        boolean f = job.waitForCompletion(true);
        if (f) {
            System.out.println("第一次执行成功。。。。。。。");
        }


    }
}
