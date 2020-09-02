package com.sys.hadoop.mr.tfidf.first;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 * Create by yang_zzu on 2020/8/29 on 16:45
 */
public class FirstPartition extends HashPartitioner<Text, IntWritable> {

    @Override
    public int getPartition(Text key, IntWritable value, int numReduceTasks) {

        /**
         * 将数据进行路由操作，指定某一类数据放到指定的 reduce 上面，形成一个文件
         */
        if (key.equals(new Text("count"))) {
            return 3;
        } else {
            return super.getPartition(key, value, numReduceTasks - 1);
        }
    }
}
