package com.sys.hadoop.mr.tq;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * 获得每个月份中，温度最好的两天的数据
 *
 * Create by yang_zzu on 2020/8/25 on 15:57
 */
public class MyTQ {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration(true);

        Job job = Job.getInstance(configuration);
        job.setJarByClass(MyTQ.class);
//        job.waitForCompletion(true);

        Path input = new Path("/data/tq/input");
        FileInputFormat.addInputPath(job,input);
        Path output = new Path("/data/tq/output");
        if (output.getFileSystem(configuration).exists(output)) {
            output.getFileSystem(configuration).delete(output, true);
        }
        FileOutputFormat.setOutputPath(job, output);



//        job.setInputFormatClass(ooo.class);


        job.setMapperClass(TMapper.class);
        job.setMapOutputKeyClass(TQ.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setPartitionerClass(TPartitioner.class);
        job.setSortComparatorClass(TSortComparator.class);

        job.setGroupingComparatorClass(TGroupingComparator.class);
        job.setReducerClass(TReducer.class);
        job.setNumReduceTasks(2);

//        job.setCombinerClass(TReducer.class);
//        job.setCombinerKeyGroupingComparatorClass(TGroupingComparator.class);

        job.waitForCompletion(true);



    }

}
