package com.imooc.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * @author louye
 * @title: HDFSWordCountApp01
 * @description:
 * 使用HDFS API完成wordCount的统计
 *
 * 需求: 统计HDFS上的文件的wordCount, 然后将统计结果输出到HDFS
 * 功能拆解:
 * 1.读取HDFS文件 ===> HDFS API
 * 2.业务处理(词频统计): 对文件中每一行数据都要进行业务处理(按照分隔符分割) ===> Mapper
 * 3.将处理结果缓存起来 ===> Context
 * 4.将结果输出到HDFS ===> HDFS API
 * @date 2019-06-24,20:21
 */
public class HDFSWordCountApp01 {

    public static void main(String[] args) throws Exception {
        //1.读取HDFS文件 ===> HDFS API
        Properties properties = ParamUtils.getProperties();
        Path input = new Path(properties.getProperty(Constants.INPUT_PATH));

        //获取要操作的HDFS文件系统
        FileSystem fs = FileSystem.get(new URI(properties.getProperty(Constants.HDFS_URI)), new Configuration(), "hadoop");
//        ImoocMapper mapper = new WordCountMapper();
        // TODO 通过反射创建对象
        Class<?> clazz = Class.forName(properties.getProperty(Constants.MAPPER_CLASS));
        ImoocMapper mapper = (ImoocMapper) clazz.newInstance();
        ImoocContext context = new ImoocContext();


        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(input, false);
        while (iterator.hasNext()) {
            LocatedFileStatus file = iterator.next();
            //获取到输入流
            FSDataInputStream in = fs.open(input);
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            String line = "";
            while ((line = reader.readLine()) != null) {
                //2.业务处理(词频统计)

                // TODO: 在业务逻辑处理完后将结果写到Cache中去
                mapper.map(line, context);

            }
            reader.close();
            in.close();
        }

        // todo 3.将处理结果缓存起来 Map

        Map<Object, Object> contextMap = context.getCacheMap();

        //4.将结果输出到HDFS ===> HDFS API
        Path output = new Path(properties.getProperty(Constants.OUTPUT_PATH));
        FSDataOutputStream out = fs.create(new Path(output, new Path(properties.getProperty(Constants.OUTPUT_FILE))));
        //todo 将第三步缓存中的内容输出到out中去
        Set<Map.Entry<Object, Object>> entries = contextMap.entrySet();
        for (Map.Entry<Object, Object> entry : entries) {
            out.write((entry.getKey().toString() + "\t" + entry.getValue() + "\n").getBytes());
        }
        out.close();
        fs.close();

        System.out.println("Louye HDFS统计词频运行成功");
    }
}
