package com.sys.es.util;

import com.sys.es.HtmlBean;
import org.apache.commons.lang.StringUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;

/**
 * Create by yang_zzu on 2020/10/17 on 9:28
 */
public class HtmlTool {

    /**
     *
     * @param path html 文件路径
     */
    public static HtmlBean parserHtml(String path)throws Throwable{
        HtmlBean bean  =new HtmlBean();
        //读取 html 文件
        FileInputStream fileInputStream = null;
        StringBuffer sb = new StringBuffer();
        try {
            fileInputStream = new FileInputStream(path);
            byte[] bytes = new byte[1024];
            while (-1 != fileInputStream.read(bytes)) {
                sb.append(new String(bytes));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                fileInputStream.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
        }
        Document document = Jsoup.parse(sb.toString());

        String title = document.title();

        if (StringUtils.isNotEmpty(title)) {
            bean.setTitle(title);
        } else {
            return null;
        }
        //获得, body 的内容
        String bodyContent = document.body().text();
        bean.setContent(bodyContent);
        // url 路径
        bean.setUrl(path);

        /*String url =path.substring(IndexService.DATA_DIR.length());
        bean.setContent(output);
        bean.setUrl(url);*/

        return bean;
    }

    /**
     * 工具类的测试方法
     */
    @Test
    public void test1() {
        try {

            HtmlBean htmlBean = parserHtml("C:\\Users\\180619\\Desktop\\cdh namenode 节点启动失败（无法打开文件）_yang_zzu的博客-CSDN博客.html");
            System.out.println(htmlBean.getTitle());
            System.out.println(htmlBean.getUrl());
            System.err.println(htmlBean.getContent());

        } catch (Throwable e) {
            e.printStackTrace();
        }
    }


}
