import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Top3StocksByVolumeCompressionDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = getConf();
		
		conf.setBoolean("mapreduce.compress.map.output", true); // Used to compress the map output.
		conf.setClass("mapreuce.map.output.compress.codec", SnappyCodec.class, CompressionCodec.class); // Setting the compression codec for map output.
		
		Job job = Job.getInstance(getConf());
		job.setJarByClass(getClass());		
				
		job.setInputFormatClass(CombineTextInputFormat.class);				
		job.setMapperClass(Top3StocksByVolumeMapper.class);
		job.setOutputKeyClass(CustomLongPair.class);
		job.setOutputValueClass(Text.class);
		
		job.setNumReduceTasks(2);		
		job.setReducerClass(Reducer.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
		
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		System.exit(ToolRunner.run(new Top3StocksByVolumeCompressionDriver(), args));
	}

}
