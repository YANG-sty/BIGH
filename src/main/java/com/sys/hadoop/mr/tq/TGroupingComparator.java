package com.sys.hadoop.mr.tq;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 分组比较器
 * Create by yang_zzu on 2020/8/25 on 19:19
 */
public class TGroupingComparator extends WritableComparator {

    public TGroupingComparator() {
        super(TQ.class, true);
    }


    /**
     * 1949 10 01 88
     * 1949 11 02 78
     * 1949 12 04 68
     * 1949 10 05 58
     */
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TQ t1 = (TQ) a;
        TQ t2 = (TQ) b;

        int compare = Integer.compare(t1.getYear(), t2.getYear());
        if (compare == 0) {
            int compare1 = Integer.compare(t1.getMonth(), t2.getMonth());
            return compare1;
        }
        return compare;
    }
}
