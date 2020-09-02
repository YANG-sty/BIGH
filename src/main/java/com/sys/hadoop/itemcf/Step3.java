package com.sys.hadoop.itemcf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.Map;

/**
 * 对物品组合列表进行计数，建立物品的同线矩阵
 * i100:i100	3
 * i100:i105	1
 * i100:i106	1
 * i100:i109	1
 * i100:i114	1
 * i100:i124	1
 *
 * Create by yang_zzu on 2020/9/1 on 11:13
 */
public class Step3 {
    public static boolean run(Configuration configuration, Map<String, String> paths) {

        try {
            FileSystem fileSystem = FileSystem.get(configuration);
            Job job = Job.getInstance(configuration);
            job.setJobName("step_3");
            job.setJarByClass(StartRun.class);

            job.setMapperClass(Step3_Mapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            job.setReducerClass(Step3_Reducer.class);

            FileInputFormat.addInputPath(job, new Path(paths.get("Step3Input")));
            Path ouputPath = new Path(paths.get("Step3Output"));
            if (fileSystem.exists(ouputPath)) {
                fileSystem.delete(ouputPath, true);
            }
            FileOutputFormat.setOutputPath(job, ouputPath);

            boolean b = job.waitForCompletion(true);

        } catch (Exception e) {
            e.printStackTrace();
        }

        return false;
    }

}
