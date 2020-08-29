package com.sys.hadoop.mr.pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 *
 * Create by yang_zzu on 2020/8/28 on 16:06
 */
public class MyPageRank {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration(true);

        //精确度
        double d = 0.0000001;
        int i = 0;
        while (true) {
            i++;

            configuration.setInt("runCount", i);
            Job job = Job.getInstance(configuration);
            job.setJarByClass(MyPageRank.class);
            job.setJobName("pr" + i);
            job.setMapperClass(PageRankMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setReducerClass(PageRankReducer.class);

            //使用新的输入格式化类
            job.setInputFormatClass(KeyValueTextInputFormat.class);

            Path inputPath = new Path("/data/pagerank/input");
            if (i > 1) {
                inputPath = new Path("/data/pagerank/output/pr" + (i - 1));
            }
            FileInputFormat.addInputPath(job, inputPath);

            Path outputPath = new Path("/data/pagerank/output/pr" + i);
            if (outputPath.getFileSystem(configuration).exists(outputPath)) {
                outputPath.getFileSystem(configuration).delete(outputPath, true);
            }
            FileOutputFormat.setOutputPath(job, outputPath);

            boolean b = job.waitForCompletion(true);
            if (b) {
                System.out.println("第 " + i + " 次执行成功---------success......");
                long sum = job.getCounters().findCounter(MyCounter.my).getValue();
                System.out.println(sum);
                double avgd = sum / 4000.0;
                //如果最终的精确度 小于 预设的精确度，则跳出循环，执行结束
                if (avgd < d) {
                    break;
                }

            }


        }


    }
}
