import java.io.IOException;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SortByFrequency extends Configured implements Tool{
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new SortByFrequency(), args);
		System.exit(exitCode);
	}
 
	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = new Configuration();
		String[] args = new GenericOptionsParser(conf,arg0).getRemainingArgs();
		//接收最小词频参数“k“
		if(args.length != 3){
			System.err.println("Usage:seg.SortByFrequency <input> <output> k");
			System.exit(2);
		}
        conf.set("k",args[2]);
		Job job = Job.getInstance(conf,"SortByFrequency");
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(new Path(args[1])))
		fs.delete(new Path(args[1]),true);
		job.setJarByClass(SortByFrequency.class);
		job.setMapperClass(SortMapper.class);
		job.setReducerClass(SortReducer.class);
		// 设置为倒序
		job.setSortComparatorClass(DescComparator.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}
 
	public static class SortMapper extends Mapper<LongWritable,Text,IntWritable,Text>{
        private int k = 0;
		protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        k = Integer.parseInt(conf.get("k"));
    };
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String str[] = value.toString().split("\t");
			context.write(new IntWritable(Integer.valueOf(str[1])), new Text(str[0]));
                        
		};
	}
	public static class SortReducer extends Reducer<IntWritable,Text,Text,IntWritable>{
        private int k = 0;
        protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        k = Integer.parseInt(conf.get("k"));
    };

		private Text result = new Text();
		public void reduce(IntWritable key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
		    for(Text val : values){
			    result.set(val);
                if(key.get()>k)
			        context.write(result, key);  
			}
		}
	}
	//设置比较规则
	public static class DescComparator extends WritableComparator{
 
		protected DescComparator() {
			super(IntWritable.class,true);
		}
 
		@Override
		public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3,
			int arg4, int arg5) {
			return -super.compare(arg0, arg1, arg2, arg3, arg4, arg5);
		}
		@Override
		public int compare(Object a,Object b){
			return -super.compare(a, b);
		}
	}
}
