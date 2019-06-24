package com.imooc.bigdata.hdfs.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;


/**
 * @author louye
 * @title: HDFSApp
 * @description: 使用Java API 操作HDFS文件系统
 *
 * 关键点:
 * 1.创建 Configuration
 * 2.获取FileSystem
 * 3.就是HDFS API的操作
 * @date 2019-06-17,23:51
 */
public class HDFSApp {
//    public static void main(String[] args) throws Exception{
////        Configuration configuration = new Configuration();
////        FileSystem fileSystem = FileSystem.get(
////                new URI("hdfs://hadoop000:8020"),
////                configuration,
////                "hadoop");
////        Path path = new Path("/hdfsapi/test");
////        boolean result = fileSystem.mkdirs(path);
////        System.out.println(result);
////    }

    public static final String HDFS_PATH = "hdfs://hadoop000:8020";
    FileSystem fileSystem = null;
    Configuration configuration = null;

    @Before
    public void setUp() throws Exception {
        System.out.println("================ setUp =============");
        configuration = new Configuration();
        configuration.set("dfs.replication", "1");
        fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, "hadoop");
    }

    @Test
    public void mkdir() throws Exception {
        fileSystem.mkdirs(new Path("/hdfsapi/test"));
    }

    @Test
    public void text() throws Exception{
        FSDataInputStream in = fileSystem.open(new Path("/README.txt"));
        IOUtils.copyBytes(in, System.out, 2048);
    }

    @Test
    public void create() throws Exception {
//        FSDataOutputStream out = fileSystem.create(new Path("/hdfsapi/test/a.txt"));
        FSDataOutputStream out = fileSystem.create(new Path("/hdfsapi/test/b.txt"));
        out.writeUTF("hello pk: replication 1");
        out.flush();
        out.close();

    }


    /**
     * 测试文件名更改
     */
    @Test
    public void rename() throws Exception {
        Path oldPath = new Path("/hdfsapi/test/b.txt");
        Path newPath = new Path("/hdfsapi/test/c.txt");
        boolean result = fileSystem.rename(oldPath, newPath);
        System.out.println(result);
    }

    /**
     * 拷贝本地文件到远程指定路径下
     * @throws Exception
     */
    @Test
    public void copyFormLocalFile() throws Exception {
        Path src = new Path("/Users/louye/dump.rdb");
        Path dst = new Path("/hdfsapi/test/");
        fileSystem.copyFromLocalFile(src, dst);
    }

    /**
     * 拷贝本地大文件到远程指定路径下: 带进度
     * @throws Exception
     */
    @Test
    public void copyFormLocalBigFile() throws Exception {
        InputStream in = new BufferedInputStream(
                new FileInputStream(new File("/Users/louye/Downloads/WPS_Office_1.1.0(1454).dmg")));
        FSDataOutputStream out = fileSystem.create(new Path("/hdfsapi/test/sprk.tar.gz"),
                new Progressable() {
                    public void progress() {
                        System.out.print(".");
                    }
                });

        IOUtils.copyBytes(in, out, 4096);
    }

    /**
     * 从HDFS拷贝文件到本地: 下载
     * @throws Exception
     */
    @Test
    public void copyToLocalFile() throws Exception {
        Path src = new Path("/hdfsapi/test/a.txt");
        Path dst = new Path("/Users/louye/tmp/tmpFile/");
        fileSystem.copyToLocalFile(src, dst);
    }

    /**
     * 查看目标文件夹下的所有文件
     * @throws Exception
     */
    @Test
    public void listFiles() throws Exception {
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            String isDir = fileStatus.isDirectory() ? "文件夹" : "文件";
            String permission = fileStatus.getPermission().toString();
            short replication = fileStatus.getReplication();
            long len = fileStatus.getLen();
            String path = fileStatus.getPath().toString();

            System.out.println(isDir + "\t" + permission
                    + "\t" + replication + "\t" + len
                    + "\t" + path
            );
        }
    }

    /**
     * 递归查看目标文件夹下的所有文件
     * @throws Exception
     */
    @Test
    public void listFilesRecursive() throws Exception {
        RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(new Path("/hdfsapi/test"), true);
        while (files.hasNext()) {
            LocatedFileStatus file = files.next();
            String isDir = file.isDirectory() ? "文件夹" : "文件";
            String permission = file.getPermission().toString();
            short replication = file.getReplication();
            long len = file.getLen();
            String path = file.getPath().toString();

            System.out.println(isDir + "\t" + permission
                    + "\t" + replication + "\t" + len
                    + "\t" + path
            );
        }
    }


    /**
     * 查看文件块信息
     * @throws Exception
     */
    @Test
    public void getFileBlockLocations() throws Exception {
        FileStatus fileStatus = fileSystem.getFileStatus(new Path("/hdfsapi/test/sprk.tar.gz"));
        BlockLocation[] blocks = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
        for (BlockLocation block : blocks) {
            for (String name : block.getNames()) {
                System.out.println(name + " : " + block.getOffset() + " : " + block.getLength());
            }
        }

    }

    /**
     * 删除文件
     * @throws Exception
     */
    @Test
    public void delete() throws Exception {
        //第二个参数是是否需要递归删除
        boolean result = fileSystem.delete(new Path("/hdfsapi/test/sprk.tar.gz"), true);
        System.out.println(result);
    }

    @After
    public void shutDown() {
        configuration = null;
        fileSystem = null;
        System.out.println("================= shutDown ===============");
    }

}
