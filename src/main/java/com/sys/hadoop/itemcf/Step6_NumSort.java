package com.sys.hadoop.itemcf;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Create by yang_zzu on 2020/9/1 on 16:16
 */
public class Step6_NumSort extends WritableComparator {
    public  Step6_NumSort() {
        super(Step6_PairWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        Step6_PairWritable o1 = (Step6_PairWritable) a;
        Step6_PairWritable o2 = (Step6_PairWritable) b;

        int r = o1.getUid().compareTo(o2.getUid());
        if (r == 0) {
            return -Double.compare(o1.getNum(), o2.getNum());
        }
        return r;
    }
}
