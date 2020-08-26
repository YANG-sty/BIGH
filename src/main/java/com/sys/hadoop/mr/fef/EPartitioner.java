package com.sys.hadoop.mr.fef;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Create by yang_zzu on 2020/8/26 on 21:11
 */
public class EPartitioner extends Partitioner<FE, IntWritable> {
    @Override
    public int getPartition(FE fe, IntWritable intWritable, int i) {
        return fe.getUser().hashCode() % i;
    }
}
