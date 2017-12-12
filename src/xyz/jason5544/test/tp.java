package xyz.jason5544.test;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class tp {
	
	public static SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	static class tpMapper extends Mapper<LongWritable, Text, KeyPair, Text>
	{
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			String[] ss = line.split("\t");
			if (ss.length == 2)
			{
				try
				{
					Date date = SDF.parse(ss[0]);
					Calendar c = Calendar.getInstance();
					c.setTime(date);
					int year = c.get(1);
					String tp  = ss[1].substring(0, ss[1].indexOf("C"));
					KeyPair kp = new KeyPair();
					kp.setTp(Integer.parseInt(tp));
					kp.setYear(year);
					context.write(kp, value);
				}
				catch (Exception e)
				{
					e.printStackTrace();
				}
			}
		}

	}

	static class tpReducer extends Reducer<KeyPair, Text, KeyPair, Text>
	{
		protected void reduce(KeyPair kp, Iterable<Text> value, Context context)
			throws IOException, InterruptedException
		{
			for (Text v: value)
			{
				context.write(kp, v);
			}
		}
	}

	
	public static void main(String[] args)
	{
		Configuration conf = new Configuration();
		try
		{
			Job job = new Job(conf);
			job.setJobName("tp");
			job.setJarByClass(tp.class);;
			job.setMapperClass(tpMapper.class);
			job.setReducerClass(tpReducer.class);
			job.setOutputKeyClass(KeyPair.class);
			job.setOutputValueClass(Text.class);
			job.setNumReduceTasks(3);
			
			job.setPartitionerClass(FirstPartition.class);
			job.setSortComparatorClass(SortTp.class);
			job.setGroupingComparatorClass(Group.class);

			FileInputFormat.addInputPath(job, new Path("/tp/input/"));
			FileOutputFormat.setOutputPath(job, new Path("/tp/output/"));
			System.exit(job.waitForCompletion(true)?0:1);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		
	}
}
