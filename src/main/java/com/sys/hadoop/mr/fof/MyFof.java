package com.sys.hadoop.mr.fof;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * 通过每个人的好友列表数据，获得间接好友的数据
 *
 * Create by yang_zzu on 2020/8/26 on 15:12
 */
public class MyFof {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration(true);
        /*configuration.set("mapreduce.app-submission.corss-paltform", "true");
        //如果分布式运行,必须打jar包
        //且,client在集群外非hadoop jar 这种方式启动,client中必须配置jar的位置
        configuration.set("mapreduce.framework.name", "local");
        //这个配置,只属于,切换分布式到本地单进程模拟运行的配置
        //这种方式不是分布式,所以不用打jar包*/
        Job job = Job.getInstance(configuration);
        job.setJarByClass(MyFof.class);

        //map
        job.setMapperClass(FMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //分区，使用默认的 hash
        //排序，使用默认的

        //reduce
        job.setReducerClass(FReduce.class);

        //input  output
        Path input = new Path("/data/fof/input");
        FileInputFormat.addInputPath(job, input);
        Path output = new Path("/data/fof/output");
        if (output.getFileSystem(configuration).exists(output)) {
            output.getFileSystem(configuration).delete(output,true);
        }
        FileOutputFormat.setOutputPath(job, output);

        //提交
        job.waitForCompletion(true);
    }
}
