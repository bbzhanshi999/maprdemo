package com.neuedu.compare;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(FlowDriver.class);
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);
        job.setMapOutputKeyClass(FlowWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowWritable.class);

        FileInputFormat.setInputPaths(job,new Path("f:/phone_status/"));
        FileOutputFormat.setOutputPath(job,new Path("f:/phone_status_2/"));

        //配置reduceTask数量
        job.setNumReduceTasks(5);
        //配置Partitioner实现类
        job.setPartitionerClass(FlowPatitioner.class);

        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }


    public static class FlowMapper extends Mapper<LongWritable, Text,FlowWritable,Text>{

        private FlowWritable k = new FlowWritable();
        private Text v = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\t");

            k.setUpFlow(Long.parseLong(split[1]));
            k.setDownFlow(Long.parseLong(split[2]));
            k.setTotalFlow(Long.parseLong(split[3]));

            v.set(split[0]);

            context.write(k,value);

        }
    }


    public static class FlowReducer extends Reducer<FlowWritable,Text,Text,FlowWritable>{
        @Override
        protected void reduce(FlowWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for(Text text:values){
                context.write(text,key);
            }
        }
    }
}
