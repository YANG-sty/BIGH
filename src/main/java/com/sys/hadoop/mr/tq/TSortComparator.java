package com.sys.hadoop.mr.tq;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 排序比较器，在buffer 环中进行
 * Create by yang_zzu on 2020/8/25 on 17:33
 */
public class TSortComparator extends WritableComparator {

    public TSortComparator() {
        super(TQ.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        TQ t1 = (TQ) a;
        TQ t2 = (TQ) b;

        int compare = Integer.compare(t1.getYear(), t2.getYear());
        if (compare == 0) {
            int compare1 = Integer.compare(t1.getMonth(), t2.getMonth());
            if (compare1 == 0) {
                //温度倒序排列，去负数
                int compare2 = Integer.compare(t1.getWd(), t2.getWd());
                return -compare2;
            }
            return compare1;
        }
        return compare;
    }
}
