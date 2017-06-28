import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;




public class Drive
{
	
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration c=new Configuration();
		
		Job j=new Job(c,"myjob");
		j.setJarByClass(Drive.class);
		
		j.setMapperClass(Map.class);
		j.setReducerClass(Reduce.class);
		
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(j,new Path(args[0]));
		FileOutputFormat.setOutputPath(j, new Path(args[1]));
		
		
		j.setInputFormatClass(TextInputFormat.class);
		j.setOutputFormatClass(TextOutputFormat.class);
		
		System.exit(j.waitForCompletion(true)?0:1);
		
	}
	

}
