package com.sys.hadoop.mr.tq;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Create by yang_zzu on 2020/8/25 on 16:56
 */
public class TQ implements WritableComparable<TQ> {

    private int year;
    private int month;
    private int day;
    private int wd;

    @Override
    public int compareTo(TQ that) {
        //日期正序
        int compare = Integer.compare(this.year, that.getYear());
        if (compare == 0) {
            int compare1 = Integer.compare(this.month, that.getMonth());
            if (compare1 == 0) {
                return Integer.compare(this.day, that.getDay());
            }
            return compare1;
        }
        return compare;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(year);
        out.writeInt(month);
        out.writeInt(day);
        out.writeInt(wd);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.year = in.readInt();
        this.month = in.readInt();
        this.day = in.readInt();
        this.wd = in.readInt();
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
    }

    public int getWd() {
        return wd;
    }

    public void setWd(int wd) {
        this.wd = wd;
    }
}
