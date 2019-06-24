package com.imooc.bigdata.hadoop.hdfs;

/**
 * @author louye
 * @title: CaseIgnoreWordCountMapper
 * @description: 自定义WordCount实现类
 * @date 2019-06-24,20:58
 */
public class CaseIgnoreWordCountMapper implements ImoocMapper {
    public void map(String line, ImoocContext context) {
        String[] words = line.toLowerCase().split("\t");
        for (String word : words) {
            Object value = context.get(word);
            if (value == null) {
                //表示没有出现过该单词
                context.write(word, 1);
            } else {
                int v = Integer.parseInt(value.toString());
                //去除单词对应的次数+1
                context.write(word, v + 1);
            }
        }
    }
}
