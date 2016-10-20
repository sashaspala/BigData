package inverted;

import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import util.StringIntegerList;
import util.StringIntegerList.StringInteger;

/**
 * This class is used for Section C.2 of assignment 1. You are supposed to run
 * the code taking the lemma index filename as input, and output being the
 * inverted index.
 */
public class InvertedIndexMapred {
	public static class InvertedIndexMapper extends Mapper<Text, Text, Text, StringInteger> {
		String articleName;
		String line;
		int comma;
		Integer count;
		Text lemma;
		StringInteger index;
		
		@Override
		public void map(Text articleId, Text indices, Context context) throws IOException,
				InterruptedException {
			// TODO: You should implement inverted index mapper here
			
			//load article name
			articleName = articleId.toString(); 
			//load lemma, count pairs
			line = indices.toString();
			
			Integer count = -1;
			
			//create regex to extract lemmas and counts from the indices
			Pattern r = Pattern.compile("<(\\w+), (\\d+)>");
			Matcher m = r.matcher(line);

			//read lemma, count pairs from indices
			while (m.find()){
				lemma = new Text(m.group(1));
				count = Integer.parseInt(m.group(2));
				
				//construct inverted index entry and write to context
				index = new StringInteger(articleName, count);
				context.write(lemma, index);
			}
			
		}
	}

	public static class InvertedIndexReducer extends
			Reducer<Text, StringInteger, Text, StringIntegerList> {
		StringIntegerList indexWritable;
		
		@Override
		public void reduce(Text lemma, Iterable<StringInteger> articlesAndFreqs, Context context)
				throws IOException, InterruptedException {
			// TODO: You should implement inverted index reducer here
			
			//load list of articles and frequencies, and add to the final index for the key lemma
			ArrayList<StringInteger> indexList = new ArrayList<StringInteger>();
			for (StringInteger articleAndFreq : articlesAndFreqs){
				indexList.add(articleAndFreq);
			}
			
			//turn final index into writable obj and write to context
			indexWritable = new StringIntegerList(indexList);
			context.write(lemma, indexWritable);
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO: you should implement the Job Configuration and Job call
		// here

		Configuration conf = new Configuration();
		//create generic options parser to read from hadoop
		GenericOptionsParser gop = new GenericOptionsParser(conf, args);
		String[] otherArgs = gop.getRemainingArgs();
		
		Job job = Job.getInstance(conf, "inverted index");
		
		//tell hadoop where to find jar, mapper, and reducer
		job.setJarByClass(InvertedIndexMapred.class);
		job.setMapperClass(InvertedIndexMapper.class);
		job.setReducerClass(InvertedIndexReducer.class);
		
		//set input and output classes
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(StringIntegerList.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//tell hadoop where to find input and where to print output
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		try{
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
		catch (ClassNotFoundException | InterruptedException e){
			e.printStackTrace();
		}

	}
}
