import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MR_WC_Demo {
    // 第一阶段Map阶段
    public static class MyMap extends Mapper<LongWritable,Text,Text,LongWritable>{
        // <LongWritable,Text,Text,LongWritable> 代表map输入和中间那输出的hadoop数据类型
        //重写map函数，在map函数中处理自己的逻辑，一行数据调用一次map函数
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            //value = shujia,spark,hadoop
            //切分 这里是处理一调数据
            //将数据转换成String
            String string = value.toString();
            //将一行数据切分成数组  [shujia,spark,hadoop]
            String[] words = string.split(",");
            for (String string2 : words) {
                // 组装key和value
                String KS = string2;
                long val = 1l;
                // Key和Value拼接写入到磁盘
                // (shujia,1)
                context.write(new Text(KS),new LongWritable(val));
            }
        }
    }
    // 根据key进行排序合并，相同的key的数据写到一起
    //============================ shuffle ======================================
    // 相同的key数据拉取到同一个reduce(函数)中，分组
    // 第二阶段: reduce阶段
    // 需要确定好你的数据输出类型
    public static class MyReduce extends Reducer<Text,LongWritable,Text,LongWritable>{
        // 重写Reducer方法
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            // hadoop {1,1}
            // 定义一个用来统计单词数量的变量
            long sum = 0l;
            for (LongWritable val : values) {
                // 迭代求和
                sum = sum + val.get();
            }
            // 将结果输出
            context.write(key,new LongWritable(sum));
        }
    }
    // 组装MapReduce
    public static void main(String[] args) throws IOException,ClassNotFoundException,InterruptedException{
        // 获取配置项
        Configuration conf = new Configuration();
        // 获取job
        Job job = Job.getInstance(conf, MR_WC_Demo.class.getSimpleName());
        // 设置打jar包的类
        job.setJarByClass(MR_WC_Demo.class);
        // 设置job名称
        job.setJobName("MR_WC_Demo");
        // 指定map和reduce的序列化类型以及任务类
        job.setMapperClass(MyMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setReducerClass(MyReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // 设置输入路径
        FileInputFormat.addInputPath(job,new Path("src/data/data.txt"));
        // 指定输出的路径
        FileOutputFormat.setOutputPath(job,new Path("src/data/xuwangjuntong"));

        // 提交任务
        job.waitForCompletion(true);
    }
}
