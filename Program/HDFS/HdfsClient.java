import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class HdfsClient {
    @Test
    public void testmkdir() throws IOException{
        // 1.获取文件系统
        Configuration conf = new Configuration();

        //2. 配置在集群上运行
        conf.set("fs.defaultFS","hdfs://192.168.106.100:9000");

        //设置副本数
        conf.set("dfs.replication","1");

        //构建FileSystem对象
        FileSystem fs = FileSystem.get(conf);

        //创建目录
        fs.mkdirs(new Path("/doubleTTPro"));

        //关闭资源
        fs.close();
    }

    // 文件上传
    @Test
    public void testCopyFromLocalFile() throws IOException{
        // 1.获取文件系统
        Configuration conf = new Configuration();

        //2. 配置在集群上运行
        conf.set("fs.defaultFS","hdfs://192.168.106.100:9000");

        //设置副本数
        conf.set("dfs.replication","2");

        //构建FileSystem对象
        FileSystem fs = FileSystem.get(conf);

        // 上传文件
        fs.copyFromLocalFile(new Path("src/data/doubleTTPro.txt"),new Path("/doubleTTPro/"));

        //关闭资源
        fs.close();

        // 参数优先级排序  1. 客户端代码中设置的值 > resources下的用户自定义配置的文件>服务器中默认的值
    }

    @Test
    public void testCopyToLocalFile() throws IOException{
        // 1.获取文件系统
        Configuration conf = new Configuration();

        //2. 配置在集群上运行
        conf.set("fs.defaultFS","hdfs://192.168.106.100:9000");

        //设置副本数
        conf.set("dfs.replication","2");

        //构建FileSystem对象
        FileSystem fs = FileSystem.get(conf);

        /**
         *  boolean delSrc 是否将原文件删除
         *  path src 要下载的文件路径
         *  path dst 值文件将下载到的路径
         *  boolean useRawLocalFIleSystem 是否开启文件校验
         */

        fs.copyToLocalFile(false,new Path("/doubleTTPro1"),new Path("src/data/doubleTTPro2.txt"),true);

        // 关闭资源
        fs.close();
    }

    // HDFS文件夹删除
    @Test
    public void testDelete() throws IOException{
        // 1.获取文件系统
        Configuration conf = new Configuration();

        //2. 配置在集群上运行
        conf.set("fs.defaultFS","hdfs://192.168.106.100:9000");

        //设置副本数
        conf.set("dfs.replication","1");

        //构建FileSystem对象
        FileSystem fs = FileSystem.get(conf);

        // 执行删除操作
        fs.delete(new Path("/doubleTTPro1"),true);

        // 关闭资源
        fs.close();
    }

    // HDFS文件名更改
    @Test
    public void testRename() throws IOException{
        // 1.获取文件系统
        Configuration conf = new Configuration();

        //2. 配置在集群上运行
        conf.set("fs.defaultFS","hdfs://192.168.106.100:9000");

        //设置副本数
        conf.set("dfs.replication","1");

        //构建FileSystem对象
        FileSystem fs = FileSystem.get(conf);

        // 修改文件名称
        fs.rename(new Path("/doubleTTPro/doubleTTPro.txt"),new Path("/doubleTTPro/doubleTTPro_01.txt"));

        // 关闭资源
        fs.close();
    }

    @Test
    public void testListFiles() throws IOException{
        // 1.获取文件系统
        Configuration conf = new Configuration();

        //2. 配置在集群上运行
        conf.set("fs.defaultFS","hdfs://192.168.106.100:9000");

        //设置副本数
        conf.set("dfs.replication","1");

        //构建FileSystem对象
        FileSystem fs = FileSystem.get(conf);

        // 获取文件详情
        RemoteIterator<LocatedFileStatus> listFile = fs.listFiles(new Path("/hadoop-2.7.6.tar.gz"), true);

        while (listFile.hasNext()){
            LocatedFileStatus status = listFile.next();
            //输出详情
            //文件名称
            System.out.println(status.getPath().getName());
            //长度
            System.out.println(status.getLen());
            //权限
            System.out.println(status.getPermission());
            //分组
            System.out.println(status.getGroup());
            //获取存储块信息
            BlockLocation[] blockLocations = status.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations){
                // 获取块存储的主机节点
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts){
                    System.out.println(host);
                }
            }
            System.out.println("-----####------");
        }
        // 关闭资源
        fs.close();
    }
    @Test
    public void testStatus() throws IOException{
        // 1.获取文件系统
        Configuration conf = new Configuration();

        //2. 配置在集群上运行
        conf.set("fs.defaultFS","hdfs://192.168.106.100:9000");

        //设置副本数
        conf.set("dfs.replication","1");

        //构建FileSystem对象
        FileSystem fs = FileSystem.get(conf);

        // 判断是文件还是文件夹
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for(FileStatus fileStatus:fileStatuses){
            // 判断是否是文件
            if (fileStatus.isFile()){
                System.out.println("f:"+fileStatus.getPath().getName());
            }else {
                System.out.println("d:"+fileStatus.getPath().getName());
            }
        }
        // 关闭资源
        fs.close();
    }

    //HDFS的I/O流操作
    @Test
    public void putFileToHdfs() throws IOException{
        // 1.获取文件系统
        Configuration conf = new Configuration();

        //2. 配置在集群上运行
        conf.set("fs.defaultFS","hdfs://192.168.106.100:9000");

        //设置副本数
        conf.set("dfs.replication","1");

        //构建FileSystem对象
        FileSystem fs = FileSystem.get(conf);

        //创建输入流
        FileInputStream fis = new FileInputStream(new File("src/data/doubleTTPro.txt"));

        //获取输出流
        FSDataOutputStream fos = fs.create(new Path("/doubleTTPro.txt"));

        //流对拷
        IOUtils.copyBytes(fis,fos,conf);
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }

    //HDFS文件下载
    @Test
    public void getFileFromHDFS() throws IOException{
        // 1.获取文件系统
        Configuration conf = new Configuration();

        //2. 配置在集群上运行
        conf.set("fs.defaultFS","hdfs://192.168.106.100:9000");

        //设置副本数
        conf.set("dfs.replication","1");

        //构建FileSystem对象
        FileSystem fs = FileSystem.get(conf);

        //获取输入流 HDFS上的路径
        FSDataInputStream fis = fs.open(new Path("/doubleTTPro.txt"));
        //获取输出流
        FileOutputStream fos = new FileOutputStream(new File("src/data/doubleTTPro1.txt"));

        // 流对拷
        IOUtils.copyBytes(fis,fos,conf);

        //关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }

    //定位文件获取
    //下载第一块
    @Test
    public void readFileSeek1() throws IOException{
        // 1.获取文件系统
        Configuration conf = new Configuration();

        //2. 配置在集群上运行
        conf.set("fs.defaultFS","hdfs://192.168.106.100:9000");

        //设置副本数
        conf.set("dfs.replication","1");

        //构建FileSystem对象
        FileSystem fs = FileSystem.get(conf);

        //获取输入流
        FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.6.tar.gz"));

        //创建输出流
        FileOutputStream fos = new FileOutputStream(new File("src/data/hadoop-2.7.6.tar.gz.part1"));

        //流的拷贝
        byte[] buf = new byte[1024];

        for(int i =0;i<1024*128;i++){
            fis.read(buf);
            fos.write(buf);
        }

        // 5.关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
        fs.close();
    }

    //下载第二块
    @Test
    public void readFileSeek2() throws IOException{
        //1.获取文件系统
        Configuration conf = new Configuration();
        //2.配置在集群上运行
        conf.set("fs.defaultFS","hdfs://192.168.106.100:9000");
        //构建FileSystem对象
        FileSystem fs = FileSystem.get(conf);
        // 打开输入流
        FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.6.tar.gz"));
        //定位输入数据的位置
        fis.seek(1024*1024*128);
        //创建输出流
        FileOutputStream fos = new FileOutputStream(new File("src/data/hadoop-2.7.6.tar.gz.part2"));
        //流的对拷
        IOUtils.copyBytes(fis,fos,conf);

        //关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
    }
}
