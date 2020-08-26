package com.sys.hadoop.mr.fef;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Create by yang_zzu on 2020/8/26 on 20:03
 */
public class FE implements WritableComparable<FE> {

    private String user;
    private String friend;
    private int quantity;

    @Override
    public int compareTo(FE that) {
        // 按照数量正序排列
        int compare = Integer.compare(this.quantity, that.quantity);
        return compare;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(user);
        out.writeUTF(friend);
        out.writeInt(quantity);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.user = in.readUTF();
        this.friend = in.readUTF();
        this.quantity = in.readInt();
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getFriend() {
        return friend;
    }

    public void setFriend(String friend) {
        this.friend = friend;
    }

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }
}
