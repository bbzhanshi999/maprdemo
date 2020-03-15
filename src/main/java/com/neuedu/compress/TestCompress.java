package com.neuedu.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.*;

public class TestCompress {


    public static void main(String[] args) throws IOException {
        //compress("f:/web.log", BZip2Codec.class);
        decompress("f:/web.log.bz2");
    }

    /**
     * 解压缩文件
     * @param filePath
     */
    private static void decompress(String filePath) throws IOException {
        //与压缩不同的是，解压缩可以通过工厂类自动判断压缩格式
        CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());
        CompressionCodec codec = factory.getCodec(new Path(filePath));

        //获取输入流
        CompressionInputStream cis = codec.createInputStream(new FileInputStream(filePath));

        //获取输出流
        FileOutputStream fos = new FileOutputStream(filePath.substring(0,filePath.lastIndexOf(".")));

        //copy流
        IOUtils.copyBytes(cis,fos,1024);

        //关流
        IOUtils.closeStream(cis);
        IOUtils.closeStream(fos);
    }

    /**
     * 按照指定格式压缩文件
     * @param filePath
     * @param codecClass
     */
    private static void compress(String filePath, Class<? extends CompressionCodec> codecClass) throws IOException {
        //获取输入流
        FileInputStream fis = new FileInputStream(filePath);

        //创建codec实例对象
        CompressionCodec codec = ReflectionUtils.newInstance(codecClass, new Configuration());

        //创建codec包装输出流,codec.getDefaultExtension()获取压缩格式后缀名
        CompressionOutputStream cos = codec.createOutputStream(new FileOutputStream(filePath + codec.getDefaultExtension()));

        //copy流
        IOUtils.copyBytes(fis,cos,1024);

        //关流
        IOUtils.closeStream(fis);
        IOUtils.closeStream(cos);
    }
}
