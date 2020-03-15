package neuedu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsClient {


    private FileSystem fs;

    @Before
    public void before() throws URISyntaxException, IOException, InterruptedException {
       // Configuration configuration = new Configuration();
       // configuration.set("dfs.replication","1");
        fs = FileSystem.get(new URI("hdfs://hadoop152:9000"), new Configuration(), "hadoop");
    }

    @After
    public void after() throws IOException {
        fs.close();
    }

    @Test
    public void testMkdirs() throws IOException, InterruptedException, URISyntaxException {

        // 1 获取文件系统
//        Configuration configuration = new Configuration();

        // 配置在集群上运行
        // configuration.set("fs.defaultFS", "hdfs://hadoop102:9000");
        // FileSystem fs = FileSystem.get(configuration);

//        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop152:9000"), configuration, "hadoop");

        // 2 创建目录
        fs.mkdirs(new Path("/cc"));

        // 3 关闭资源,尤其在写入时，hadoop hdfs不支持并发写入
//        fs.close();
    }

    @Test
    public void put() throws IOException {
        fs.copyFromLocalFile(new Path("f:/1.txt"), new Path("/3.txt"));
    }

    @Test
    public void get() throws IOException {
        fs.copyToLocalFile(new Path("/wcinput"), new Path("f:/"));
    }

    @Test
    public void rename() throws IOException {
        fs.rename(new Path("/wcoutput"), new Path("/haha"));
    }

    @Test
    public void delete() throws IOException {
        boolean delete = fs.delete(new Path("/1.txt"), true /*是否递归删除文件和文件夹*/);
        System.out.println(delete?"删除成功":"删除失败");
    }

    @Test
    public void append() throws IOException {
        FSDataOutputStream append = fs.append(new Path("/1.txt"), 1024);
        FileInputStream fis = new FileInputStream("f:/2.txt");
        IOUtils.copyBytes(fis,append,1024);
        append.close();
        fis.close();
    }

    /**
     * 获取文件和文件夹信息
     * @throws IOException
     */
    @Test
    public void ls() throws IOException {
        FileStatus[] statuses = fs.listStatus(new Path("/"));

        for(FileStatus status:statuses){
            if(status.isFile()){
                System.out.println("---文件信息---");
                System.out.println(status.getPath());
                System.out.println(status.getLen());
            }else{
                System.out.println("----文件夹信息--------");
                System.out.println(status.getPath());
            }
        }
    }

    /**
     * 只获取文件信息
     * @throws IOException
     */
    @Test
    public void listFiles() throws IOException {
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true /*递归将文件夹中的文件也取出*/);

        while(listFiles.hasNext()){
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println("---文件信息----------");
            System.out.println(fileStatus.getPath());

            //获取块信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            System.out.println("-------块信息---------");
            for(BlockLocation bl:blockLocations){
                String[] hosts = bl.getHosts();
                for(String host:hosts){
                    System.out.println(host+"  ");
                }
            }
        }
    }

    /**
     * 定位读取文件
     */
    @Test
    public void readFileSeek1() throws IOException {
        FSDataInputStream in = fs.open(new Path("/hadoop-3.2.1.tar.gz"));

        FileOutputStream out = new FileOutputStream("f:/hadoop-3.2.1.tar.gz.1");

        byte[] buffer =new byte[1024];

        for(int i=0;i<1024*128;i++){
            in.read(buffer);
            out.write(buffer);
        }
        IOUtils.closeStream(in);
        IOUtils.closeStream(out);
    }

    @Test
    public void readFileSeek2() throws IOException, URISyntaxException, InterruptedException {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop152:9000"), configuration, "hadoop");
        FSDataInputStream in = fs.open(new Path("/hadoop-3.2.1.tar.gz"));
        // 3定位输入数据位置
        in.seek(1024*1024*128);
        FileOutputStream out = new FileOutputStream("f:/hadoop-3.2.1.tar.gz.2");
        IOUtils.copyBytes(in,out,configuration);
        IOUtils.closeStream(in);
        IOUtils.closeStream(out);
    }
}
