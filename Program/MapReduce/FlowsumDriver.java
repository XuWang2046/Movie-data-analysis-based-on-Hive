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

public class FlowsumDriver {
    public static class FlowCountMapper extends Mapper<LongWritable, Text,Text,FlowBean>{
        FlowBean v = new FlowBean();
        Text k = new Text();

        // 重写map方法
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {
            // 1.获取第一行  1	13736230513	192.196.100.1	www.atguigu.com	2481	24681	200
            String line = value.toString();
            // 2.对字段进行切割
            String[] fields = line.split("\t");
            // 3.封装对象
            // 取出手机号码
            String phoneNum = fields[1];
            // 取出上行流量和下行流量
            long upFlow = Long.parseLong(fields[fields.length-3]);
            long downFlow = Long.parseLong(fields[fields.length-2]);

            k.set(phoneNum);
            v.set(upFlow,downFlow);

            // 4 写入磁盘
            context.write(k,v);
        }
    }

    public static class FlowCountReducer extends Reducer<Text,FlowBean,Text,FlowBean>{
        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context) throws IOException, InterruptedException {
            long sum_upFlow = 0;
            long sum_downFlow = 0;
            // (13736230513, (flowBean,flowBean,flowBean))
            // 1遍历所有bean 将其中的上行流量，下行流程分别累加
            for (FlowBean flowBean : values) {
                sum_upFlow += flowBean.getUpFlow();
                sum_downFlow += flowBean.getDownFlow();
            }
            // 2 封装对象
            FlowBean resultBean = new FlowBean(sum_upFlow,sum_downFlow);

            // 3.结果写出
            context.write(key,resultBean);
        }
    }
    public static void main(String[] args) throws IOException,ClassNotFoundException,InterruptedException{
        // 配置文件
        Configuration conf = new Configuration();
        // 创建job任务
        Job job = Job.getInstance(conf, FlowsumDriver.class.getSimpleName());
        // 设置打包类
        job.setJarByClass(FlowsumDriver.class);
        job.setJobName("zhongqihang");

        // 指定Map类和reduce类
        job.setMapperClass(FlowCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        job.setReducerClass(FlowCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        // 指定输入路径和输出路径
        FileInputFormat.setInputPaths(job,new Path("src/data/phone_data.txt"));
        FileOutputFormat.setOutputPath(job,new Path("src/data/output"));

        //提交任务
        job.waitForCompletion(true);
    }
}
