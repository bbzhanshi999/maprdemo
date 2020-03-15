package com.neuedu.friends;

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

public class StageOneDriver {
    
     public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
             Job job = Job.getInstance(new Configuration());
             job.setJarByClass(StageOneDriver.class);
             
             job.setMapperClass(StageOneMapper.class);
             job.setReducerClass(StageTwoReducer.class);
     
             job.setMapOutputKeyClass(Text.class);
             job.setMapOutputValueClass(Text.class);
     
             job.setOutputKeyClass(Text.class);
             job.setOutputValueClass(Text.class);
     
             FileInputFormat.setInputPaths(job, new Path("f:/input"));
             FileOutputFormat.setOutputPath(job, new Path("f:/output"));
     
             boolean result = job.waitForCompletion(true);
             System.exit(result ? 0 : 1);
         }



    public static class StageOneMapper extends Mapper<LongWritable, Text,Text,Text> {

        private  Text k = new Text();
        private  Text v = new Text();

        /**
         * 输出每个人的好友，有一个输出一个，例如：A B
         * 由于好友是单向的，因此要将key设置为冒号右边的人
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //1以冒号切割
            String[] relations = value.toString().split(":");
            String  person = relations[0];
            String[] friends = relations[1].split(",");

            v.set(person);
            for(String friend:friends){
                k.set(friend);
                context.write(k, v);
            }
        }
    }


    public static class StageTwoReducer extends Reducer<Text,Text,Text,Text>{

        private  Text v = new Text();

        /**
         * 输出
         * @param key
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            StringBuilder sb = new StringBuilder();

            for(Text value:values){
                sb.append(value).append(",");
            }
            sb.replace(sb.length()-1,sb.length(),"");

            v.set(sb.toString());
            context.write(key, v);
        }
    }
}
