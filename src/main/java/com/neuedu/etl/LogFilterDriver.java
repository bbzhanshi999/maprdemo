package com.neuedu.etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class LogFilterDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();
        // 开启map端输出压缩
        //configuration.setBoolean("mapreduce.map.output.compress",true);
        // 设置map端输出压缩方式
       // configuration.setClass("mapreduce.map.output.compress.codec", BZip2Codec.class, CompressionCodec.class);


        Job job = Job.getInstance(configuration);
        job.setJarByClass(LogFilterDriver.class);

        job.setMapperClass(LogFilterMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        System.out.println("hello ");
        String a = "a";
        String b = "b";
        Integer c = 12;
        // 设置reduce端输出压缩开启
        FileOutputFormat.setCompressOutput(job, true);

        // 设置压缩的方式
        FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);

        FileInputFormat.setInputPaths(job, new Path("f:/web.log"));
        FileOutputFormat.setOutputPath(job, new Path("f:/output"));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }


    public static class LogFilterMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

        private Text k = new Text();
        private BooleanWritable v = new BooleanWritable(false);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (!value.toString().contains(" 200 ")) {
                context.getCounter("logValid", "false").increment(1);
                context.write(value, NullWritable.get());
            } else {
                context.getCounter("logValid", "true").increment(1);

            }
        }
    }
}
