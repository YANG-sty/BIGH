package com.sys.es;

import org.apache.commons.lang.StringEscapeUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.safety.Whitelist;
import org.junit.Test;
import org.owasp.html.ElementPolicy;
import org.owasp.html.HtmlPolicyBuilder;
import org.owasp.html.PolicyFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

/**
 * Create by yang_zzu on 2020/10/17 on 9:28
 */
public class MyHtmlTool {

    public String sanitizeHtml(String htmlContent) {

        //允许的标签
        String[] allowedTags = {"h1", "h2", "h3", "h4", "h5", "h6",
                "span", "strong",
                "img", "video", "source",
                "blockquote", "p", "div",
                "ul", "ol", "li",
                "table", "thead", "caption", "tbody", "tr", "th", "td", "br",
                "a"
        };

        //需要转化的标签
        String[] needTransformTags = {"article", "aside", "command", "datalist", "details", "figcaption", "figure",
                "footer", "header", "hgroup", "section", "summary"};

        //带有超链接的标签
        String[] linkTags = {"img", "video", "source", "a"};

       /* PolicyFactory policy = new HtmlPolicyBuilder()
                //所有允许的标签
                .allowElements(allowedTags)
                //内容标签转化为div
                .allowElements( new ElementPolicy() {
                    @Override
                    public String apply(String elementName, List<String> attributes){
                        return "div";
                    }
                },needTransformTags)
                .allowAttributes("src","href","target").onElements(linkTags)
                //校验链接中的是否为http
                .allowUrlProtocols("https")
                .toFactory();*/
        PolicyFactory policy = new HtmlPolicyBuilder()
                //校验链接中的是否为http
                .toFactory();
        String safeHTML = policy.sanitize(htmlContent);
        return safeHTML;
    }

    @Test
    public void test1() {
        //读取 html 文件
        FileInputStream fileInputStream = null;
        StringBuffer sb = new StringBuffer();
        try {
            fileInputStream = new FileInputStream("C:\\Users\\180619\\Desktop\\cdh namenode 节点启动失败（无法打开文件）_yang_zzu的博客-CSDN博客.html");
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

        String s = sanitizeHtml(sb.toString());
        System.out.println(sb.toString());

    }


    @Test
    public void tets3() {

        //读取 html 文件
        FileInputStream fileInputStream = null;
        StringBuffer sb = new StringBuffer();
        try {
            fileInputStream = new FileInputStream("C:\\Users\\180619\\Desktop\\cdh namenode 节点启动失败（无法打开文件）_yang_zzu的博客-CSDN博客.html");
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

//        document.outputSettings(new Document.OutputSettings().prettyPrint(false));

        //输出 html 的全部数据，没有格式，
        String output = document.body().text();
        System.out.println(output);



        System.err.println("=============================================================");

        //输出 HTML 的全部数据，有部分的格式
        Document.OutputSettings outputSettings = new Document.OutputSettings().prettyPrint(false);
        document.outputSettings(outputSettings);
        document.select("br").append("");
        document.select("p").prepend("\\n");
        document.select("p").append("");
        document.select("hr").append("");

        //获得 body 标签中的 HTML 文件
        String newHtml = document.body().html().replaceAll("\\\\n", "");
        /**
         * newHtml 原生的HTML数据
         * baseUri 将html中相对路径转换为绝对路径的URL
         * Whitelist 白名单，允许的html标签和属性    做标签清洗，主要作用，防止 xss（跨站脚本） 攻击
         * outputSettings 文档输出设置，控制精细打印
         */
        String plainText = Jsoup.clean(newHtml, "", Whitelist.none(), outputSettings);
        String result = StringEscapeUtils.unescapeHtml(plainText.trim());

        System.out.println(result);

    }


    @Test
    public void test2() {
        System.out.println("abc");
    }





    /*public static HtmlBean parserHtml(String path)throws Throwable{
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
        //将 HTML 的string 字符串转为 document 对象
        Document doc = Jsoup.parse(sb.toString());

        String title = doc.select("title").text();
        bean.setTitle(title);
        return null;

    }*/


}
