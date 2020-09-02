package com.sys.hadoop.mr.tfidf.last;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Map;

/**
 * Create by yang_zzu on 2020/8/30 on 20:37
 */
public class LastMapper extends Mapper<LongWritable, Text, Text, Text> {

    //存放微博总数
    public static Map<String, Integer> cmap = null;
    //存放 df
    public static Map<String, Integer> df = null;

    //在map 方法执行之前

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        System.out.println("**********************");
        if (cmap == null || cmap.size() == 0 || df == null || df.size() == 0) {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null) {
                for (int i = 0; i < cacheFiles.length; i++) {
                    URI uri = cacheFiles[i];
                    if (uri.getPath().endsWith("part-r-00003")) {//微博总数
                        Path path = new Path(uri.getPath());
                        // FileSystem fs
                        // =FileSystem.get(context.getConfiguration());
                        // fs.open(path);
                        BufferedReader bufferedReader = new BufferedReader(new FileReader(path.getName()));
                        String s = bufferedReader.readLine();
                        if (s.startsWith("count")) {
                            String[] split = s.split("\t");
                            cmap = new HashMap<String, Integer>();
                            cmap.put(split[0], Integer.parseInt(split[1].trim()));
                        }
                        bufferedReader.close();
                    } else if (uri.getPath().endsWith("part-r-00000")) {// 词条的DF
                        df = new HashMap<String, Integer>();
                        Path path = new Path(uri.getPath());
                        BufferedReader bufferedReader = new BufferedReader(new FileReader(path.getName()));
                        String line;
                        while ((line = bufferedReader.readLine()) != null) {
                            String[] split = line.split("\t");
                            df.put(split[0], Integer.parseInt(split[1].trim()));
                        }
                        bufferedReader.close();
                    }
                }
            }

        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        FileSplit fileSplit = (FileSplit) context.getInputSplit();

        if (!fileSplit.getPath().getName().contains("part-r-00003")) {
            //豆浆_3823930429533207	2
            String[] split = value.toString().trim().split("\t");
            if (split.length >= 2) {
                int tf = Integer.parseInt(split[1].trim());//tf 值
                String[] s = split[0].split("_");
                if (s.length >= 2) {
                    String w = s[0];
                    String id = s[1];
                    double count = tf * Math.log(cmap.get("count") / df.get(w));
                    NumberFormat instance = NumberFormat.getInstance();
                    instance.setMaximumFractionDigits(5);
                    context.write(new Text(id), new Text(w + ":" + instance.format(count)));

                }
            } else {
                System.out.println(value.toString()+"-----------------");
            }
        }
    }
}
