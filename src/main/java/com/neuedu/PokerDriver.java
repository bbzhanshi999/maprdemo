package com.neuedu;

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
import java.util.concurrent.atomic.AtomicInteger;


public class PokerDriver  {


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1. 获取配置信息以及封装任务
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        //2. 设置jar加载路径为PokerDriver自己所在的jar包
        job.setJarByClass(PokerDriver.class);

        //3.设置map和reduce类
        job.setMapperClass(PokerMapper.class);
        job.setReducerClass(PokerReducer.class);

        //4、设置map输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //5. 设置最终输出也就是reducer的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //6.设置输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //7.提交并返回是否成功 verbose代表是否打印过程日志给用户
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }

    /**
     * Mapper类：LongWritable指的是每一行数据开头距离整个文件开头有多少个字符，也就是距文件开头的偏移量
     */
    private static class PokerMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
        Text k = new Text();
        IntWritable v = new IntWritable(1);


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //mapper的职责是将每一张牌按照花色分类，反映到代码就是key：花色，value：1
            // 1.获取一行
            String line = value.toString();

            // 2.切割
            String[] cards = line.split(" ");

            for(String card:cards){
                if (card.length()>3) {
                    k.set(card.substring(0, 2));
                    context.write(k, v);
                }

            }

        }




    }

    /**
     * Reduce类：处理map输出的key，value
     */
    private static class PokerReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        IntWritable v = new IntWritable(1);


        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            AtomicInteger sum = new AtomicInteger();
            values.forEach(val-> sum.addAndGet(val.get()));

            v.set(sum.intValue());
            context.write(key,v);
        }
    }
}
