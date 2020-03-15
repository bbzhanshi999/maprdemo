package com.neuedu.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TwoIndexDriver {

     public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
             Job job = Job.getInstance(new Configuration());
             job.setJarByClass(TwoIndexDriver.class);

             job.setMapperClass(TwoIndexMapper.class);
             job.setReducerClass(TwoIndexReducer.class);

             job.setMapOutputKeyClass(Text.class);
             job.setMapOutputValueClass(Text.class);

             job.setOutputKeyClass(Text.class);
             job.setOutputValueClass(Text.class);

             FileInputFormat.setInputPaths(job, new Path(args[0]));
             FileOutputFormat.setOutputPath(job, new Path(args[1]));

             boolean result = job.waitForCompletion(true);
             System.exit(result ? 0 : 1);
         }


    public static class TwoIndexMapper extends Mapper<LongWritable,Text, Text,Text>{
        private Text k = new Text();
        private Text v = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\t");
            String num = split[1];

            //neuedu--a.txt
            String[] wordAndFile = split[0].split("--");
            String word  = wordAndFile[0];
            String fileName = wordAndFile[1];

            k.set(word);
            v.set(fileName+"-->"+num);
            context.write(k,v);

        }
    }

    public static class TwoIndexReducer extends Reducer<Text,Text,Text,Text>{
        private Text v = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();

            for(Text value:values){
                sb.append(value).append("\t");
            }
            v.set(sb.toString());
            context.write(key, v);
        }
    }
}
