package com.sys.hadoop.mr.fef;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 排序比较器
 * 在这里对key value 进行排序, 整个计算中唯一的一次排序，其他的都是在组内进行排序
 *
 * Create by yang_zzu on 2020/8/26 on 20:17
 */
public class ESortComparator extends WritableComparator {

    public ESortComparator() {
        super(FE.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        FE e1 = (FE) a;
        FE e2 = (FE) b;

        int i = e1.getUser().compareTo(e2.getUser());
        if (i == 0) {
            //倒序，数值大的在前
            return -Integer.compare(e1.getQuantity(), e2.getQuantity());
        }
        return i;
    }

}
