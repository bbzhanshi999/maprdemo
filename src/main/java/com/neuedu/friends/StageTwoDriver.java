package com.neuedu.friends;

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
import java.util.Arrays;
import java.util.Collections;

public class StageTwoDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(StageTwoDriver.class);

        job.setMapperClass(StageTwoMapper.class);
        job.setReducerClass(StageTwoReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path("f:/output"));
        FileOutputFormat.setOutputPath(job, new Path("f:/output2"));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }


    public static class StageTwoMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text k = new Text();
        private Text v = new Text();

        /**
         * 输入数据的key就是value锁拥有的共同好友
         *
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] relations = value.toString().split("\t");
            String person = relations[0];
            String[] friends = relations[1].split(",");

            //输出数据格式： A-B: L
            v.set(person);

            //将friends的排序按照字符串大小来排序；
            Arrays.sort(friends);


            for (int i = 0; i < friends.length; i++) {
                for (int j = i + 1; j < friends.length; j++) {
                    k.set(friends[i] + "-" + friends[j]);
                    context.write(k, v);
                }
            }

        }

    }


    public static class StageTwoReducer extends Reducer<Text, Text, Text, Text> {

        private Text v = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();

            for (Text value : values) {
                sb.append(value + " ");
            }
            v.set(sb.toString());
            context.write(key, v);
        }
    }
}
