package com.sys.hadoop.itemcf;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 * Create by yang_zzu on 2020/9/1 on 16:08
 */
public class Step6_Mapper extends Mapper<LongWritable, Text, Step6_PairWritable, Text>  {

    Step6_PairWritable mkey = new Step6_PairWritable();

    Text mvalue = new Text();

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
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = Pattern.compile("[\t,]").split(value.toString());
        String u = split[0];
        String item = split[1];
        String num = split[2];

        mkey.setUid(u);
        mkey.setNum(Double.parseDouble(num));

        mvalue.set(item + ":" + num);

        context.write(mkey, mvalue);
        /**
         * u367     30     i101:30
         */
    }
}
