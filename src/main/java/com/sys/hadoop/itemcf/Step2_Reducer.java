package com.sys.hadoop.itemcf;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Create by yang_zzu on 2020/9/1 on 10:53
 */
public class Step2_Reducer extends Reducer<Text, Text, Text, Text> {

    Map<String, Integer> map = new HashMap<>();
    /**
     *  u2625  i161:1
     *  u2625  i161:2
     *  u2625  i161:4
     *  u2625  i162:3
     *  u2625  i161:4
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //清空 map 集合的数据
        map.clear();

        for (Text value : values) {
            String[] split = value.toString().split(":");
            String item = split[0];
            int action = Integer.parseInt(split[1]);
            //如果该产品在map 集合中不存在，则初始值设为 0 ，然后加上 评分值
            action = (map.get(item) == null ? 0 : map.get(item).intValue()) + action;

            map.put(item, action);
        }
        StringBuffer stringBuffer = new StringBuffer();
        for (Map.Entry<String, Integer> stringIntegerEntry : map.entrySet()) {
            stringBuffer.append(stringIntegerEntry.getKey() + ":" + stringIntegerEntry.getValue() + ",");
        }

        map.clear();

        context.write(key, new Text(stringBuffer.toString()));
        // u2625    i161:8,i162:3,

    }
}
