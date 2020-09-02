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
 * Create by yang_zzu on 2020/9/1 on 15:57
 */
public class Step6 {
    public static boolean run(Configuration configuration, Map<String, String> paths) {

        try {
            FileSystem fileSystem = FileSystem.get(configuration);
            Job job = Job.getInstance(configuration);
            job.setJobName("step_6");
            job.setJarByClass(StartRun.class);

            job.setMapperClass(Step6_Mapper.class);
            job.setMapOutputKeyClass(Step6_PairWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setSortComparatorClass(Step6_NumSort.class);

            job.setGroupingComparatorClass(Step6_UserGroup.class);

            job.setReducerClass(Step6_Reducer.class);

            FileInputFormat.addInputPath(job, new Path(paths.get("Step6Input")));
            Path outpath = new Path(paths.get("Step6Output"));
            if (fileSystem.exists(outpath)) {
                fileSystem.delete(outpath, true);
            }
            FileOutputFormat.setOutputPath(job, outpath);

            boolean f = job.waitForCompletion(true);
            return f;


        } catch (Exception e) {
            e.printStackTrace();
        }


        return false;

    }
}
