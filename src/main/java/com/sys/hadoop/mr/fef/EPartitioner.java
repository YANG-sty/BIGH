package com.sys.hadoop.mr.fef;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 分区比较器，将相同属性的数据放到一个分区中，
 * 分区比较器的主要作用是，防止数据倾斜，将小数据放到一起达到和大数据一样的数据集
 *
 * Create by yang_zzu on 2020/8/26 on 21:11
 */
public class EPartitioner extends Partitioner<FE, IntWritable> {
    @Override
    public int getPartition(FE fe, IntWritable intWritable, int i) {
        return fe.getUser().hashCode() % i;
    }
}
