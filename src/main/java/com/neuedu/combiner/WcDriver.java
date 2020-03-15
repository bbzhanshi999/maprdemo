package com.neuedu.combiner;

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
import java.util.Iterator;

public class WcDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        job.setMapperClass(WcMapper.class);
        job.setReducerClass(WcReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setCombinerClass(WcReducer.class);

        FileInputFormat.setInputPaths(job, new Path("f:/input"));
        FileOutputFormat.setOutputPath(job, new Path("f:/output"));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }


    public static class WcMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Text k = new Text();
        private IntWritable v = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split(" ");

            for (String word : words) {
                k.set(word);
                context.write(k, v);
            }

        }
    }


    public static class WcReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable v = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            v.set(sum);
            context.write(key, v);
        }
    }
}
