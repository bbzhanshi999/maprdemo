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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class OneIndexDriver {

     public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
             Job job = Job.getInstance(new Configuration());
             job.setJarByClass(OneIndexDriver.class);

             job.setMapperClass(OneIndexMapper.class);
             job.setReducerClass(OneIndexReducer.class);

             job.setMapOutputKeyClass(Text.class);
             job.setMapOutputValueClass(IntWritable.class);

             job.setOutputKeyClass(Text.class);
             job.setOutputValueClass(IntWritable.class);

             FileInputFormat.setInputPaths(job, new Path(args[0]));
             FileOutputFormat.setOutputPath(job, new Path(args[1]));

             boolean result = job.waitForCompletion(true);
             System.exit(result ? 0 : 1);
         }


    public static class OneIndexMapper extends Mapper<LongWritable, Text,Text, IntWritable>{

        private String fileName;
        private Text k = new Text();
        private IntWritable v = new IntWritable();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
           FileSplit split = (FileSplit) context.getInputSplit();

           fileName = split.getPath().getName();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split(" ");
            //设置key 单词--文件名
            for(String word:words){
                k.set(word+"--"+fileName);
                v.set(1);
                context.write(k, v);
            }
        }
    }

    public static class OneIndexReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        private IntWritable v = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //累加计算每个key的值得和
            int sum = 0;
            for(IntWritable value:values){
                sum += value.get();
            }
            v.set(sum);
            context.write(key, v);
        }
    }
}
