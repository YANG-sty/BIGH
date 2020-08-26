package com.sys.hadoop.mr.fef;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 分组比较器
 * Create by yang_zzu on 2020/8/26 on 20:23
 */
public class EGroupComparator extends WritableComparator {

    public EGroupComparator() {
        super(FE.class, true);
    }

    /**
     * 相同的key 为一组
     */
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        FE e1 = (FE) a;
        FE e2 = (FE) b;
        int to = e1.getUser().compareTo(e2.getUser());
        return to;
    }

}
