package com.sys.hadoop.itemcf;

import com.google.inject.internal.asm.$TypePath;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.Map;

/**
 *
 * 按用户分组，计算所有物品出现的组合列表，得到用户对物品的喜爱度得分矩阵
 * u13	i160:1,
 * u14	i25:1,i223:1,
 * u16	i252:1,
 * u21	i266:1,
 * u24	i64:1,i218:1,i185:1,
 * u26	i276:1,i201:1,i348:1,i321:1,i136:1,
 *
 * Create by yang_zzu on 2020/9/1 on 10:38
 */
public class Step2 {

    public static boolean run(Configuration configuration, Map<String, String> paths) {
        try {

            FileSystem fileSystem = FileSystem.get(configuration);
            Job job = Job.getInstance(configuration);
            job.setJobName("step_2");
            job.setJarByClass(StartRun.class);

            job.setMapperClass(Step2_Mapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setReducerClass(Step2_Reducer.class);

            FileInputFormat.addInputPath(job, new Path(paths.get("Step2Input")));
            Path ouputPath = new Path(paths.get("Step2Output"));
            /*if (fileSystem.exists(ouputPath)) {
                fileSystem.delete(ouputPath, true);
            }*/
            if (ouputPath.getFileSystem(configuration).exists(ouputPath)) {
                ouputPath.getFileSystem(configuration).delete(ouputPath, true);
            }
            FileOutputFormat.setOutputPath(job, ouputPath);

            boolean b = job.waitForCompletion(true);
            return b;

        } catch (Exception e) {
            e.printStackTrace();
        }

        return false;
    }

}
