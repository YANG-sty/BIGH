package com.sys.hadoop.itemcf;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Create by yang_zzu on 2020/9/1 on 14:29
 */
public class Step4_Reducer extends Reducer<Text, Text, Text, Text> {

    /**
     * 和该物品（key中的itemID）同现的其他物品的同现集合 // 。
     * 其他物品ID为map的key，同现数字为值
     */
    Map<String, Integer> mapB = new HashMap<String, Integer>();
    // 该物品（key中的itemID），所有用户的推荐权重分数。
    Map<String, Integer> mapA = new HashMap<String, Integer>();

    Text rkey = new Text();
    Text rvalue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //清除 map 集合中的数据
        mapA.clear();
        mapB.clear();

        /**
         * A同现矩阵 or B得分矩阵
         * 某一个物品，针对它和其他所有物品的同现次数，都在mapA集合中
         */
        for (Text line : values) {
            String value = line.toString();
            if (value.startsWith("A:")) {
                /**
                 * i469     A:i469,1
                 * i469     A:i498,2
                 * i469     A:i154,2
                 * i469     A:i73 ,3
                 * i469     A:i162,1
                 */
                String[] split = Pattern.compile("[\t,]").split(value.substring(2));

                /**
                 * i469     1
                 * i498     2
                 * i154     2
                 * i73      3
                 * i162     1
                 */
                mapA.put(split[0], Integer.parseInt(split[1]));
            } else if (value.startsWith("B:")) {

                /**
                 * 相同的 key 为一组
                 *
                 * i276    B:u26,1
                 * i276    B:u33,1
                 * i276    B:u41,2
                 *
                 * i321    B:u26,3
                 * i321    B:u33,1
                 */
                String[] split = Pattern.compile("[\t,]").split(value.substring(2));

                /**
                 * u26      1
                 * u33      1
                 * u41      2
                 *
                 * u26      3
                 * u33      1
                 */
                mapB.put(split[0], Integer.parseInt(split[1]));

            }
        }

        double result = 0;

        Iterator<String> iterator = mapA.keySet().iterator();
        while (iterator.hasNext()) {
            // itemID
            String mapka = iterator.next();
            //对于 i276 这个产品的同现次数
            int num = mapA.get(mapka).intValue();

            Iterator<String> iterb = mapB.keySet().iterator();
            while (iterb.hasNext()) {
                // userID
                String mapkb = iterb.next();
                int pref = mapB.get(mapkb).intValue();
                //矩阵乘法相乘计算
                result = num * pref;

                rkey.set(mapkb);
                rvalue.set((mapka + "," + result));

                /**
                 * u367     i101,3
                 * u231     i101,2
                 * u232     i101,2
                 *
                 * u367     i102,3
                 * u231     i102,1
                 * u232     i102,2
                 */
                context.write(rkey, rvalue);
            }
        }
    }
}
