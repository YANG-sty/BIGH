package com.sys.hadoop.mr.tfidf.last;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Create by yang_zzu on 2020/8/30 on 20:19
 */
public class LastJob {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration(true);
        Job job = Job.getInstance(configuration);

        job.setJarByClass(LastJob.class);
        job.setJobName("weibo_3");

        //把微博总数加载到
        job.addCacheFile(new Path("/data/tfidf/output/weibo_1/part-r-00003").toUri());
        //把df加载到
        job.addCacheFile(new Path("/data/tfidf/output/weibo_2/part-r-00000").toUri());

        job.setMapperClass(LastMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setReducerClass(LastReduce.class);

        Path inputPath = new Path("/data/tfidf/output/weibo_1");
        FileInputFormat.addInputPath(job, inputPath);

        Path outputPath = new Path("/data/tfidf/output/weibo_3");
        if (outputPath.getFileSystem(configuration).exists(outputPath)) {
            outputPath.getFileSystem(configuration).delete(outputPath, true);
        }
        FileOutputFormat.setOutputPath(job, outputPath);

        boolean b = job.waitForCompletion(true);
        if (b) {
            System.out.println("第三次执行成功。。。。。。。");
        }

    }
}
