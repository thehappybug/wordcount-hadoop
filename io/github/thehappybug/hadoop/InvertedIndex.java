package io.github.thehappybug.hadoop;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class InvertedIndex 
{
	public static class InvertedIndex_Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
	{
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException 
		{
			FileSplit fileSplit = (FileSplit)reporter.getInputSplit();
			Text fileName = new Text(fileSplit.getPath().getName());

			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) 
			{
				output.collect(new Text(tokenizer.nextToken()), fileName);
			}
		}
	}

	public static class InvertedIndex_Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> 
	{
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException 
		{
			HashSet<String> files = new HashSet<String>();
			StringBuilder paths = new StringBuilder();
			int counter = 0;
			while (values.hasNext())
			{
				String filename = values.next().toString();
				if(!(files.contains(filename)))
				{  
					files.add(filename);
					if (counter > 0)
						paths.append(",  " + filename);

					else
						paths.append(filename);
					counter++;
				}
			}

			output.collect(key, new Text(paths.toString()));
		}
	}

	public static void main(String[] args) throws Exception 
	{
		JobConf conf = new JobConf(InvertedIndex.class);
		conf.setJobName("Inverted Index");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(InvertedIndex_Map.class);
		conf.setCombinerClass(InvertedIndex_Reduce.class);
		conf.setReducerClass(InvertedIndex_Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}

}