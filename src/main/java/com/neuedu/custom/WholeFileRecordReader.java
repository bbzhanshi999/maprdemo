package com.neuedu.custom;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class WholeFileRecordReader extends RecordReader<Text, BytesWritable> {

    private boolean isRead = false;
    private Text key = new Text();
    private BytesWritable value = new BytesWritable();
    private FileSplit fileSplit;
    private FSDataInputStream fis;

    /**
     * 初始化方法，hadoop集群在创建完RecordReader后悔调用该方法进行自定义初始化
     * 创建一个输入流以便于nextKeyValue方法通过流读取文件内容，并转换成BytesWritable，设置给value
     *
     * @param split
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        fileSplit = (FileSplit) split;
        Path path = fileSplit.getPath();

        //创建切片的输入流
        FileSystem fileSystem = path.getFileSystem(context.getConfiguration());
        fis = fileSystem.open(path);

    }

    /**
     * 读取文件下一段数据，并返回是否读取陈宫，类似迭代器
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!isRead){
            //读取key
            key.set(fileSplit.getPath().toString());


            //读取文件作为value
            byte[] buff = new byte[(int) fileSplit.getLength()];
            value.set(buff,0,buff.length);
            isRead = true;
            return true;
        }
        return false;
    }

    /**
     * 获取key值
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    /**
     * 获取value值
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    /**
     * 获取读取进度
     *
     * @return 以0-1之间的一个浮点型数表示读取进度，0未读，1读完，
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return isRead ? 1 : 0;
    }

    /**
     * 关闭所需资源，如io流对象等
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        IOUtils.closeStream(fis);
    }
}
