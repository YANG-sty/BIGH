package com.sys.hadoop.mr.pagerank;

import org.apache.commons.lang3.StringUtils;

/**
 * Create by yang_zzu on 2020/8/29 on 9:23
 */
public class ABC {
    public static void main(String[] args) {
         char fieldSeparator = '\t';

        String a = "aaaaa";
        String b = "bbbbb";
        String c = "ccccc";

        String x = a + fieldSeparator + b + fieldSeparator + c;
        System.out.println(x);
        String[] strings = StringUtils.split(x, fieldSeparator);
        for (String string : strings) {
            System.out.println(string);
        }
        boolean boo = false;
        if (boo) {
            System.out.println("1111");

        }

    }
}
