package com.imooc.bigdata.hadoop.mr.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author louye
 * @title: WordCountMapper
 * @description:
 * KEYIN: Map任务读数据的key类型, offset, 每行数据起始位置的偏移量, Long
 * VALUEIN: Map任务读数据的value类型, 起始就是一行行的字符串, String
 *
 * hello  world welcome
 * hello welcome
 *
 * KEYOUT: Map方法自定义实现输出的key的类型
 * VALUEOUT: Map方法自定义实现输出的value的类型
 *
 * 词频统计: 相同单词的次数 (world, 1)
 *
 * Long, String, String, Integer是Java里面的数据类型
 * Hadoop自定义类型: 序列化和反序列化
 *
 * LongWritable, Text
 * @date 2019-06-25,09:40
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //把value对应的行数据按照指定的分隔符拆开
        String[] words = value.toString().split("\t");
        for (String word : words) {
            //(hello, 1) (world, 1)
            context.write(new Text(word.toLowerCase()), new IntWritable(1));
        }
    }
}
