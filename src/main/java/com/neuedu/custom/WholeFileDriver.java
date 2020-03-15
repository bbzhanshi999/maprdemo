package com.neuedu.custom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class WholeFileDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        job.setMapOutputValueClass(BytesWritable.class);
        job.setMapOutputKeyClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        //输出format采用SequenceFileOutputFormat
        job.setInputFormatClass(WholeFileInputFormat.class);
        //设置输入format采用自定义format
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path("f:/input"));
        FileOutputFormat.setOutputPath(job, new Path("f:/output"));

        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);

    }
}
