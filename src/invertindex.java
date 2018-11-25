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

public class invertindex extends Configured implements Tool{
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new invertindex(), args);
		System.exit(exitCode);
	}
 
	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = new Configuration();
		// 第三个参数为索引的词语。
		String[] args = new GenericOptionsParser(conf,arg0).getRemainingArgs();
		if(args.length != 3){
			System.err.println("Usage:seg.invertindex <input> <output> word");
			System.exit(2);
		}
		conf.set("words",args[2]);
		Job job = Job.getInstance(conf,"invertindex");
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(new Path(args[1])))
			fs.delete(new Path(args[1]),true);
		job.setJarByClass(invertindex.class);
		job.setMapperClass(SegmentMapper.class);
		
		//job.setCombinerClass(SegReducer.class);
		job.setReducerClass(SegReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}
 
	public static class SegmentMapper extends Mapper<LongWritable,Text,Text,Text>{
		private Text key = new Text();
        private String words = "";
		//接受参数words
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            words = conf.get("words");
        };	
		public void map(LongWritable offset,Text value,Context context) throws IOException, InterruptedException{
			String[] str = value.toString().split("\\s+");
                if(str.length == 6){ 
                    //对新闻标题分词并去除英文和数字				
			        List<Term> list = HanLP.segment(str[4].replaceAll("[a-zA-Z0-9]", ""));
					// url
                    String url = str[5];
					// 股票代码
				    String code = str[0];
			        for(Term a : list){
                        String s = a.toString().split("/")[0];
						// 键值对为<单词+股票代码，url+one>进行map
                        if(s.equals(words)){
				            key.set(s+","+code);
			         	    value.set(url+","+"1");
			                context.write(key, value);
                        }
			        }
                }
		}
	}
	public static class SegReducer extends Reducer<Text,Text,Text,IntWritable>{
		private IntWritable result = new IntWritable();
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
			int sum = 0;
			String url_list="";
			String[] keys = key.toString().split(",");
			String code = keys[1];
			String word = keys[0];
			for(Text val : values){
				String[] value = val.toString().split(",");
				sum += Integer.parseInt(value[1]);
				if(url_list.length() == 0)
					url_list = value[0];
				else
					url_list = url_list + "," + value[0];
			}
			Text index = new Text();
			// 键值对为<单词+股票代码,[url列表]，词频>输出
		    index.set(word + "," + code + ",[" + url_list + "]");
			result.set(sum);
			context.write(index, result);
		}
	}

}
