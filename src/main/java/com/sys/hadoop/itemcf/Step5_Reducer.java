package com.sys.hadoop.itemcf;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Create by yang_zzu on 2020/9/1 on 15:41
 */
public class Step5_Reducer extends Reducer<Text, Text, Text, Text> {

    // 结果
    Map<String, Double> map = new HashMap<String, Double>();

    Text mvalue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //清除map 数据
        map.clear();

        /**
         * key      values
         * u367     i101,3
         * u231     i101,2
         * u232     i101,2
         *
         * u367     i102,3
         * u231     i102,1
         * u232     i102,2
         */
        //相同的 key 为一组
        for (Text value : values) {
            String[] split = value.toString().split(",");
            String itemID = split[0];
            double score = Double.parseDouble(split[1]);

            if (map.containsKey(itemID)) {
                // 矩阵乘法求和计算
                map.put(itemID, map.get(itemID) + score);
            } else {
                map.put(itemID, score);
            }
        }

        Iterator<String> iterator = map.keySet().iterator();
        while (iterator.hasNext()) {
            String itemID = iterator.next();
            Double score = map.get(itemID);
            mvalue.set(itemID + "," + score);

            context.write(key, mvalue);
            /**
             * key      values
             * u367     i101,30
             * u231     i101,20
             * u232     i101,21
             *
             * u367     i102,32
             * u231     i102,11
             * u232     i102,21
             */
        }

    }
}
