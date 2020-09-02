package com.sys.hadoop.itemcf;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 * Create by yang_zzu on 2020/9/1 on 13:59
 */
public class Step4_Mapper extends Mapper<LongWritable, Text, Text, Text> {

    // A同现矩阵 or B得分矩阵
    private String flag;

    Text rkey = new Text();
    Text rvalue = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        //判断读取的数据集
        flag = fileSplit.getPath().getParent().getName();

        System.out.println(flag + "************************");

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        try {
            String[] tokens = Pattern.compile("[\t,]").split(value.toString());
            //同线矩阵
            if (flag.equals("step3")) {
                /**
                 * i469:i469    1
                 * i469:i498    2
                 * i469:i154    2
                 * i469:i73     3
                 * i469:i162    1
                 */
                String[] splits = tokens[0].split(":");
                String itemID1 = splits[0];
                String itemID2 = splits[1];
                String num = tokens[1];

                rkey.set(itemID1);
                rvalue.set("A:" + itemID2 + "," + num);

                context.write(rkey, rvalue);
                /**
                 * i469     A:i469,1
                 * i469     A:i498,2
                 * i469     A:i154,2
                 * i469     A:i73 ,3
                 * i469     A:i162,1
                 */

                // 用户对物品喜爱得分矩阵
            } else if (flag.equals("step2")) {
                //u26	i276:1,i201:1,i348:1,i321:1,i136:1,
                String userID = tokens[0];
                for (int i = 1; i < tokens.length; i++) {
                    String[] splits = tokens[i].split(":");
                    //物品id
                    String itemID = splits[0];
                    //分值
                    String pref = splits[1];

                    rkey.set(itemID);
                    rvalue.set("B:" + userID + "," + pref);

                    context.write(rkey, rvalue);
                    /**
                     * i276    B:u26,1
                     * i201    B:u26,1
                     * i384    B:u26,2
                     * i321    B:u26,3
                     * i136    B:u26,1
                     */
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
