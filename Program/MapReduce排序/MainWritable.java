import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.io.Reader;

public class MainWritable {
    // mapreduce本身就会根据key对数据进行排序，所以在这里，不需要value的输出，因为value的内容排序由FlowBeans完成了，我们只需要输出FlowBeans就可以了
    public static class SortMap extends Mapper<LongWritable, Text,FlowBeans, NullWritable>{
        // 定义一个对象
        FlowBeans bean = new FlowBeans();

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, FlowBeans, NullWritable>.Context context) throws IOException, InterruptedException {
            //0000001,Shujia_01,222.8
            //将数据转换成String类型
            String s = value.toString();
            //对数据进行切分
            String[] split = s.split(",");
            // 将需要的数据拿出来，并且放到实体类里面去
            bean.setId(Long.parseLong(split[0]));
            bean.setPrice(Double.parseDouble(split[2]));
            // 输出数据
            context.write(bean,NullWritable.get());
        }
    }
    public static class SortReduce extends Reducer<FlowBeans,NullWritable,FlowBeans,NullWritable>{
        // 在reduce端 无需做任何操作，直接输出即可

        @Override
        protected void reduce(FlowBeans key, Iterable<NullWritable> values, Reducer<FlowBeans, NullWritable, FlowBeans, NullWritable>.Context context) throws IOException, InterruptedException {
            context.write(key,NullWritable.get());
        }
    }
    public static void main(String[] args) throws IOException,ClassNotFoundException,InterruptedException{
        // 配置文件
        Configuration conf = new Configuration();
        // 创建job任务
        Job job = Job.getInstance(conf, MainWritable.class.getSimpleName());
        // reduce的个数是可以人为去设置的
        job.setNumReduceTasks(2);
        // 设置打包类
        job.setJarByClass(MainWritable.class);

        //指定map类和Reduce类的输出类型
        job.setMapperClass(SortMap.class);
        job.setMapOutputKeyClass(FlowBeans.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setReducerClass(SortReduce.class);
        job.setOutputKeyClass(FlowBeans.class);
        job.setOutputValueClass(NullWritable.class);

        // 指定数据的输入路径和输出路径
        FileInputFormat.setInputPaths(job,new Path("src/data/orders.txt"));
        FileOutputFormat.setOutputPath(job,new Path("src/data/output2"));

        //提交任务
        job.waitForCompletion(true);
    }
}
