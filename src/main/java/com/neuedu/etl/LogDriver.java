package com.neuedu.etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class LogDriver {

     public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
             Job job = Job.getInstance(new Configuration());
             job.setJarByClass(LogDriver.class);

             job.setMapperClass(LogMapper.class);

             job.setMapOutputKeyClass(Text.class);
             job.setMapOutputValueClass(BooleanWritable.class);

             FileInputFormat.setInputPaths(job, new Path("f:/web.log"));
             FileOutputFormat.setOutputPath(job, new Path("f:/output"));

             boolean result = job.waitForCompletion(true);
             System.exit(result ? 0 : 1);
         }



    public static class LogMapper extends Mapper<LongWritable, Text,Text, BooleanWritable>{

        private Text k = new Text();
        private BooleanWritable v = new BooleanWritable(false);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(" ");
            for(String field:split){
                k.set(field);
                if(field.length()>11){
                    v.set(true);
                    context.getCounter("logValid","true").increment(1);
                }else{
                    v.set(true);
                    context.getCounter("logValid","false").increment(1);
                }
                context.write(k,v);
            }
        }
    }
}
