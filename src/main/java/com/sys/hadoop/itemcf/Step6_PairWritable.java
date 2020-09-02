package com.sys.hadoop.itemcf;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Create by yang_zzu on 2020/9/1 on 16:04
 */
public class Step6_PairWritable implements WritableComparable<Step6_PairWritable> {

    private String uid;
    private Double num;

    @Override
    public int compareTo(Step6_PairWritable o) {
        int i = this.uid.compareTo(o.getUid());
        if (i == 0) {
            return Double.compare(this.num, o.getNum());
        }
        return i;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(uid);
        out.writeDouble(num);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.uid = in.readUTF();
        this.num = in.readDouble();
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public Double getNum() {
        return num;
    }

    public void setNum(Double num) {
        this.num = num;
    }
}
