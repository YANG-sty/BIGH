package com.sys.hadoop.itemcf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;

/**
 * Create by yang_zzu on 2020/9/1 on 10:25
 */
public class Step1 {

    public static boolean run(Configuration configuration, Map<String, String> paths) {
        try {
            FileSystem fileSystem = FileSystem.get(configuration);
            Job job = Job.getInstance(configuration);
            job.setJobName("step_1");
            job.setJarByClass(StartRun.class);

            job.setMapperClass(Step1_Mapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(NullWritable.class);

            job.setReducerClass(Step1_Reducer.class);

            FileInputFormat.addInputPath(job, new Path(paths.get("Step1Input")));
            Path ouputPaht = new Path(paths.get("Step1Output"));
            /*if (fileSystem.exists(ouputPaht)) {
                fileSystem.delete(ouputPaht, true);
            }*/
            if (ouputPaht.getFileSystem(configuration).exists(ouputPaht)) {
                ouputPaht.getFileSystem(configuration).delete(ouputPaht, true);
            }

            FileOutputFormat.setOutputPath(job, ouputPaht);

            boolean b = job.waitForCompletion(true);
            return b;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }
}
