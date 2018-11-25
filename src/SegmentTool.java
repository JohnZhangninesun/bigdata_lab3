import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
// 引入Hanlp中文分词类
import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.seg.common.Term; 

public class SegmentTool extends Configured implements Tool{
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new SegmentTool(), args);
		System.exit(exitCode);
	}
 
	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = new Configuration();
		String[] args = new GenericOptionsParser(conf,arg0).getRemainingArgs();
		if(args.length != 2){
			System.err.println("Usage:seg.SegmentTool <input> <output>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf,"SegmentTool");
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(new Path(args[1])))
			fs.delete(new Path(args[1]),true);
		job.setJarByClass(SegmentTool.class);
		job.setMapperClass(SegmentMapper.class);
		job.setCombinerClass(SegReducer.class);
		job.setReducerClass(SegReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}
 
	public static class SegmentMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
		private Text word = new Text();	
		private final static IntWritable one = new IntWritable(1);
		public void map(LongWritable offset,Text value,Context context) throws IOException, InterruptedException{
	        System.out.println(value);
		    // 将每行数据分开，得到新闻标题
			String[] str = value.toString().split("\\s+");
                if(str.length>=2){
                //将新闻标题分词           
			    List<Term> list = HanLP.segment(str[str.length-2].replaceAll("[a-zA-Z0-9]", ""));
                        
			    for(Term a : list){
                    String s = a.toString().split("/")[0];
					//默认中文词是至少两个，可以改
                    if(s.length()>1){
				        word.set(s);
					    ontext.write(word, one);
                    }
				}
            }
		}
	}
	public static class SegReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
		private IntWritable result = new IntWritable();
		public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
			int sum = 0;
			//汇总统计词频
			for(IntWritable val : values)
				sum += val.get();
			result.set(sum);
			context.write(key, result);
		}
	}

}
