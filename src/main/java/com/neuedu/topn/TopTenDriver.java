package com.neuedu.topn;

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
import java.util.TreeMap;

public class TopTenDriver {
     public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
             Job job = Job.getInstance(new Configuration());
             job.setJarByClass(TopTenDriver.class);

             job.setMapperClass(TopTenMapper.class);
             job.setReducerClass(TopTenReducer.class);

             job.setMapOutputKeyClass(FlowWritable.class);
             job.setMapOutputValueClass(Text.class);

             job.setOutputKeyClass(Text.class);
             job.setOutputValueClass(FlowWritable.class);

             FileInputFormat.setInputPaths(job, new Path("f:/input"));
             FileOutputFormat.setOutputPath(job, new Path("f:/output"));

             boolean result = job.waitForCompletion(true);
             System.exit(result ? 0 : 1);
         }
    
    public static class TopTenMapper extends Mapper<LongWritable, Text, FlowWritable, Text> {

        private FlowWritable k = new FlowWritable(); //流量对象
        private Text v = new Text(); //电话号码

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //封装bean
            String[] split = value.toString().split("\t");

            String phoneNum = split[0];
            v.set(phoneNum);
            k.setUpFlow(Long.parseLong(split[1]));
            k.setDownFlow(Long.parseLong(split[2]));
            k.setTotalFlow(Long.parseLong(split[3]));

            context.write(k,v);
        }
    }

    public static class TopTenReducer extends Reducer<FlowWritable,Text,Text,FlowWritable> {

        private TreeMap<FlowWritable,Text> tempMap = new TreeMap<>();

        @Override
        protected void reduce(FlowWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            FlowWritable flowWritable = new FlowWritable();
            flowWritable.setUpFlow(key.getUpFlow());
            flowWritable.setDownFlow(key.getDownFlow());
            flowWritable.setTotalFlow(key.getTotalFlow());

            tempMap.put(flowWritable,values.iterator().next());

            if(tempMap.size()>10){
                tempMap.remove(tempMap.lastKey());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Iterator<FlowWritable> keys = tempMap.keySet().iterator();
            while(keys.hasNext()){
                FlowWritable value = keys.next();
                context.write(tempMap.get(value),value);
            }
        }
    }
}
