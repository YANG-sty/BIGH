package com.sys.hadoop.mr.fef;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * 使用 fof 产生的间接好友的数据，生成每个人的好友推荐的顺序
 *
 * Create by yang_zzu on 2020/8/26 on 19:38
 */
public class MyFef {

    public static void main(String[] args) throws Exception {


        Configuration configuration = new Configuration(true);
        Job job = Job.getInstance(configuration);
        job.setJarByClass(MyFef.class);

        job.setMapperClass(EMapper.class);
        job.setMapOutputKeyClass(FE.class);
        job.setMapOutputValueClass(IntWritable.class);

        //分区，使用默认的 hash 就可以
        /**
         * 排序，按照推荐指数排序，
         * cat:hadoop   3
         * cat:hello    2
         * cat:hiv  1
         */
        job.setPartitionerClass(EPartitioner.class);
        job.setSortComparatorClass(ESortComparator.class);
        //分组，相同的key 为一组
        job.setGroupingComparatorClass(EGroupComparator.class);
        job.setReducerClass(EReduucer.class);


        Path input = new Path("/data/fef/input");
        FileInputFormat.addInputPath(job,input);
        Path output = new Path("/data/fef/output");
        if (output.getFileSystem(configuration).exists(output)) {
            output.getFileSystem(configuration).delete(output, true);
        }
        FileOutputFormat.setOutputPath(job, output);

        job.waitForCompletion(true);

    }

}
