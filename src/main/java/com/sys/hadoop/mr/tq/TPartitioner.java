package com.sys.hadoop.mr.tq;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Create by yang_zzu on 2020/8/25 on 17:23
 */
public class TPartitioner extends Partitioner<TQ, IntWritable> {
    /**
     * 对 key 进行分组处理，每条数据都要调用 getPartition
     * @param tq key值
     * @param intWritable value值
     * @param i partition的个数
     * @return
     */
    @Override
    public int getPartition(TQ tq, IntWritable intWritable, int i) {
        /**
         * 在实际的操作中，为了避免数据的倾斜，需要对数据做抽样处理
         * 如果有很多组的数据，有的组数据量大，有的数据量小
         * 将数据量小的组，放到一个reduce 中
         * 将数据量大的最，放到一个reduce 中
         */
        return tq.getYear() % i;
    }
}
