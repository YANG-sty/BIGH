package com.sys.hadoop.itemcf;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Create by yang_zzu on 2020/9/1 on 16:22
 */
public class Step6_Reducer extends Reducer<Step6_PairWritable, Text, Text, Text> {

    int i = 0;
    StringBuffer stringBuffer = new StringBuffer();

    Text mkey = new Text();
    Text mvalue = new Text();

    @Override
    protected void reduce(Step6_PairWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        i = 0;
        //清空 stringbuffer 中的数据
        stringBuffer.setLength(0);
        for (Text value : values) {
            if (i == 10) {
                break;
            }
            stringBuffer.append(value.toString() + ",");
            i++;
        }

        mkey.set(key.getUid());
        mvalue.set(stringBuffer.toString());

        context.write(mkey, mvalue);
        /**
         *
         */
    }
}
