package com.sys.hadoop.itemcf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.Map;

/**
 * Create by yang_zzu on 2020/9/1 on 13:47
 */
public class Step4 {

    public static boolean run(Configuration configuration, Map<String, String> paths) {

        try {
            FileSystem fileSystem = FileSystem.get(configuration);
            Job job = Job.getInstance(configuration);
            job.setJobName("step_4");
            job.setJarByClass(StartRun.class);

            job.setMapperClass(Step4_Mapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setReducerClass(Step4_Reducer.class);

            // 输入的数据源是 两种
            Path[] pathList = new Path[2];
            pathList[0] = new Path(paths.get("Step4Input1"));
            pathList[1] = new Path(paths.get("Step4Input2"));
            FileInputFormat.setInputPaths(job, pathList);
            Path outputPath = new Path(paths.get("Step4Output"));
            if (fileSystem.exists(outputPath)) {
                fileSystem.delete(outputPath, true);
            }
            FileOutputFormat.setOutputPath(job, outputPath);

            boolean b = job.waitForCompletion(true);

            return b;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return false;

    }
}
