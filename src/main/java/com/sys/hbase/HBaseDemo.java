package com.sys.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;


public class HBaseDemo {

    Admin admin;
    Table table;
    String TN = "phone";
    Connection connection;

    @Before
    public void init() throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        /* 这里填写 zookeeper 集群的 ip */
        configuration.set("hbase.zookeeper.quorum", "yang101,yang102,yang103");
        connection = ConnectionFactory.createConnection(configuration);
        admin = connection.getAdmin();
        table = connection.getTable(TableName.valueOf(TN));
    }

    @Test
    public void creatTable() throws Exception {

        TableName tableName = TableName.valueOf(TN);
        if (admin.tableExists(tableName)) {
            //删除表
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }

        // 表描述
        HTableDescriptor desc = new HTableDescriptor(tableName);
        HColumnDescriptor cf = new HColumnDescriptor("cf".getBytes());
        desc.addFamily(cf);
        admin.createTable(desc);
    }

    @Test
    public void insertDB() throws Exception {
        String rowKey = "1231231312";
        Put put = new Put(rowKey.getBytes());
        put.addColumn("cf".getBytes(), "name".getBytes(), "xiaohong".getBytes());
        put.addColumn("cf".getBytes(), "age".getBytes(), "23".getBytes());
        put.addColumn("cf".getBytes(), "sex".getBytes(), "women".getBytes());
        table.put(put);
    }

    /**
     * 通过 rowkey 查询数据
     */
    @Test
    public void getDB() throws Exception {
        String rowKey = "1231231312";
        Get get = new Get(rowKey.getBytes());
        get.addColumn("cf".getBytes(), "name".getBytes());
        get.addColumn("cf".getBytes(), "age".getBytes());
        get.addColumn("cf".getBytes(), "sex".getBytes());
        get.addColumn("cf".getBytes(), "phone".getBytes());
        Result rs = table.get(get);
        Cell cell = rs.getColumnLatestCell("cf".getBytes(), "name".getBytes());
        Cell cell2 = rs.getColumnLatestCell("cf".getBytes(), "age".getBytes());
        Cell cell3 = rs.getColumnLatestCell("cf".getBytes(), "sex".getBytes());
        Cell cell4 = rs.getColumnLatestCell("cf".getBytes(), "phone".getBytes());

        System.out.println(new String(CellUtil.cloneValue(cell)));
        System.out.println(new String(CellUtil.cloneValue(cell2)));
        System.out.println(new String(CellUtil.cloneValue(cell3)));
        System.out.println(new String(CellUtil.cloneValue(cell4)));

    }

    /**
     * 扫描表中的全部数据
     */
    @Test
    public void scanPhone() throws IOException {
        Scan scan = new Scan();
        ResultScanner resutScanner = table.getScanner(scan);
        for (Result result : resutScanner) {
            Cell cell = result.getColumnLatestCell("cf".getBytes(), "name".getBytes());
            Cell cell2 = result.getColumnLatestCell("cf".getBytes(), "age".getBytes());
            Cell cell3 = result.getColumnLatestCell("cf".getBytes(), "sex".getBytes());

            System.out.print(new String(CellUtil.cloneValue(cell)) + "  ");
            System.out.print(new String(CellUtil.cloneValue(cell2)) + "  ");
            System.out.println(new String(CellUtil.cloneValue(cell3)));
        }
    }

    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

    /**
     * 有10个用户，每个用户随机产生100条记录
     *
     * @throws Exception
     */
    @Test
    public void insertDB2() throws Exception {
        List<Put> puts = new ArrayList<Put>();
        for (int i = 0; i < 10; i++) {
            String phoneNum = getPhoneNum("186");
            for (int j = 0; j < 100; j++) {
                String dnum = getPhoneNum("158");
                String length = r.nextInt(99) + "";
                //拨打电话为1， 接听电话为0
                String type = r.nextInt(2) + "";
                String dateStr = getDate("2020");

                String rowkey = phoneNum + "_" + (Long.MAX_VALUE - sdf.parse(dateStr).getTime());

                Put put = new Put(rowkey.getBytes());
                put.addColumn("cf".getBytes(), "dnum".getBytes(), dnum.getBytes());
                put.addColumn("cf".getBytes(), "length".getBytes(), length.getBytes());
                put.addColumn("cf".getBytes(), "type".getBytes(), type.getBytes());
                put.addColumn("cf".getBytes(), "date".getBytes(), dateStr.getBytes());

                puts.add(put);
            }
        }

        table.put(puts);
    }

    /**
     * 通过指定 rowkey 的范围获得数据
     *
     * @throws Exception
     */
    @Test
    public void scan() throws Exception {
        //产生 rowkey
        String phoneNum = "18694881974";
        String startRow = phoneNum + "_" + (Long.MAX_VALUE - sdf.parse("20200801000000").getTime());
        String stopRow = phoneNum + "_" + (Long.MAX_VALUE - sdf.parse("20200201000000").getTime());
        Scan scan = new Scan();
        scan.setStartRow(startRow.getBytes());
        scan.setStopRow(stopRow.getBytes());
        ResultScanner rss = table.getScanner(scan);
        for (Result rs : rss) {
            System.out
                    .print(new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf".getBytes(), "dnum".getBytes()))));
            System.out.print("-"
                    + new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf".getBytes(), "length".getBytes()))));
            System.out.print(
                    "-" + new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf".getBytes(), "type".getBytes()))));
            System.out.println(
                    "-" + new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf".getBytes(), "date".getBytes()))));
        }
    }

    /**
     * rowkey开头的行
     * 列簇某个属性
     * 查询某个手机号 主叫为1 的所有记录
     *
     * @throws Exception
     */
    @Test
    public void scan2() throws Exception {
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        //以 18694881974 开头所有rowkey 行的数据
        PrefixFilter filter1 = new PrefixFilter("18694881974".getBytes());
        // type 的值为 1 的数据
        SingleColumnValueFilter filter2 = new SingleColumnValueFilter("cf".getBytes(), "type".getBytes(),
                CompareOp.EQUAL, "1".getBytes());
        list.addFilter(filter1);
        list.addFilter(filter2);
        Scan scan = new Scan();
        scan.setFilter(list);
        ResultScanner rss = table.getScanner(scan);
        for (Result rs : rss) {
            System.out
                    .print(new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf".getBytes(), "dnum".getBytes()))));
            System.out.print("-"
                    + new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf".getBytes(), "length".getBytes()))));
            System.out.print(
                    "-" + new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf".getBytes(), "type".getBytes()))));
            System.out.println(
                    "-" + new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf".getBytes(), "date".getBytes()))));
        }
    }

    /**
     * 186开头的rowkey的行
     *
     * @throws Exception
     */
    @Test
    public void scan3() throws Exception {
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        PrefixFilter filter1 = new PrefixFilter("186".getBytes());
        list.addFilter(filter1);
        Scan scan = new Scan();
        scan.setFilter(list);
        ResultScanner rss = table.getScanner(scan);
        for (Result rs : rss) {
            System.out
                    .print(new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf".getBytes(), "dnum".getBytes()))));
            System.out.print("-"
                    + new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf".getBytes(), "length".getBytes()))));
            System.out.print(
                    "-" + new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf".getBytes(), "type".getBytes()))));
            System.out.println(
                    "-" + new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf".getBytes(), "date".getBytes()))));
        }
    }

    /**
     * 列簇某个属性，所有时间节点的数据
     */
    @Test
    public void scan4() throws IOException {
        Get get = new Get(Bytes.toBytes("18688662873_9223370447081157807"));
        get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("length")); // 获取指定列族和列修饰符对应的列
        Result result = table.get(get);
        for (Cell kv : result.listCells()) {
            System.out.println("family:" + Bytes.toString(kv.getFamilyArray()));
            System.out.println("qualifier:" + Bytes.toString(kv.getQualifierArray()));
            System.out.println("value:" + Bytes.toString(kv.getValueArray()));
            System.out.println("Timestamp:" + kv.getTimestamp());
            System.out.println("-------------------------------------------");
        }

    }

    @Test
    public void insertDB3() throws Exception {
        List<Put> puts = new ArrayList<Put>();
        for (int i = 0; i < 10; i++) {
            String phoneNum = getPhoneNum("182");
            for (int j = 0; j < 100; j++) {
                String dnum = getPhoneNum("158");
                String length = r.nextInt(99) + "";
                String type = r.nextInt(2) + "";
                String dateStr = getDate("2020");
                String rowkey = phoneNum + "_" + (Long.MAX_VALUE - sdf.parse(dateStr).getTime());
                Phone.PhoneDetail.Builder phoneDetail = Phone.PhoneDetail.newBuilder();
                phoneDetail.setDate(dateStr);
                phoneDetail.setDnum(dnum);
                phoneDetail.setLength(length);
                phoneDetail.setType(type);
                Put put = new Put(rowkey.getBytes());
                put.addColumn("cf".getBytes(), "phoneDetail".getBytes(), phoneDetail.build().toByteArray());
                puts.add(put);
            }
        }
        table.put(puts);
    }

    @Test
    public void getDB2() throws Exception {
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        PrefixFilter filter1 = new PrefixFilter("182".getBytes());
        list.addFilter(filter1);
        Scan scan = new Scan();
        scan.setFilter(list);
        ResultScanner rss = table.getScanner(scan);
        for (Result rs : rss) {
            Phone.PhoneDetail phoneDetail = Phone.PhoneDetail.parseFrom(CellUtil.cloneValue(rs.getColumnLatestCell("cf".getBytes(), "phoneDetail".getBytes())));
            System.out.println(phoneDetail.toString());
        }

    }


    /**
     * 有十个用户，每个用户每天产生100条记录，将100条记录放到一个集合进行存储
     *
     * @throws Exception
     */
    @Test
    public void insertDB4() throws Exception {
        List<Put> puts = new ArrayList<Put>();
        for (int i = 0; i < 100; i++) {
            String phoneNum = getPhoneNum("162");
            String dateStr = getDate("2020");
            String rowkey = phoneNum + "_" + (Long.MAX_VALUE - sdf.parse(dateStr).getTime());
            Phone.dayPhoneDetail.Builder dayPhone = Phone.dayPhoneDetail.newBuilder();
            for (int j = 0; j < 10; j++) {
                String dnum = getPhoneNum("158");
                String length = r.nextInt(99) + "";
                String type = r.nextInt(2) + "";
                Phone.PhoneDetail.Builder phoneDetail = Phone.PhoneDetail.newBuilder();
                phoneDetail.setDate(dateStr);
                phoneDetail.setDnum(dnum);
                phoneDetail.setLength(length);
                phoneDetail.setType(type);
                dayPhone.addDayPhoneDetail(phoneDetail);
            }
            Put put = new Put(rowkey.getBytes());
            put.addColumn("cf".getBytes(), "phoneDetail".getBytes(), dayPhone.build().toByteArray());
            puts.add(put);
        }
        table.put(puts);
    }

    @Test
    public void getDB3() throws Exception {
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        PrefixFilter filter1 = new PrefixFilter("162".getBytes());
        list.addFilter(filter1);
        Scan scan = new Scan();
        scan.setFilter(list);
        ResultScanner rss = table.getScanner(scan);
        for (Result rs : rss) {
            Phone.dayPhoneDetail dayPhoneDetail = Phone.dayPhoneDetail.parseFrom(CellUtil.cloneValue(rs.getColumnLatestCell("cf".getBytes(), "phoneDetail".getBytes())));
            System.out.println(dayPhoneDetail.toString());
        }

    }


    private String getDate(String year) {
        return year + String.format("%02d%02d%02d%02d%02d",
                new Object[]{r.nextInt(12) + 1, r.nextInt(31) + 1, r.nextInt(24), r.nextInt(60), r.nextInt(60)});
    }

    private String getDate2(String yearMonthDay) {
        return yearMonthDay
                + String.format("%02d%02d%02d", new Object[]{r.nextInt(24), r.nextInt(60), r.nextInt(60)});
    }

    Random r = new Random();

    /**
     * 生成随机的手机号码
     *
     * @param string
     * @return
     */
    private String getPhoneNum(String string) {
        return string + String.format("%08d", r.nextInt(99999999));
    }


    @After
    public void destory() throws Exception {
        if (admin != null) {
            admin.close();
        }
    }




}
