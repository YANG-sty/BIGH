package com.sys.hadoop.mr.pagerank;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Create by yang_zzu on 2020/8/28 on 16:25
 */
public class PageRankMapper extends Mapper<Text, Text, Text, Text> {

    /**
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

        int runCount = context.getConfiguration().getInt("runCount", 1);
        /**
         * A	B	D
         * K:A
         * V:B D
         * K:A
         * V:0.3 B D
         */
        String page = key.toString();
        System.out.println("pageRankMapper key is key >>>>>>>>>>>> " + page);
        System.out.println("pageRankMapper key is value >>>>>>>>>>>> " + value.toString());

        Node node = null;
        if (runCount == 1) {
            node = Node.fromMR("1.0", value.toString());
        } else {
            node = Node.fromMR(value.toString());
        }

        /**
         * A:1.0 B D  传递老的pr值和对应的页面关系
         */
        context.write(new Text(page), new Text(node.toString()));

        if (node.containsAdjacentNodes()) {
            double outValue = node.getPageRank() / node.getAdjacentNodeNames().length;
            for (int i = 0; i < node.getAdjacentNodeNames().length; i++) {
                String outPage = node.getAdjacentNodeNames()[i];
                // B:0.5
                // D:0.5    页面A投给谁，谁作为key，val是票面值，票面值为：A的pr值除以超链接数量
                context.write(new Text(outPage), new Text(outValue + ""));
            }
        }

    }
}
