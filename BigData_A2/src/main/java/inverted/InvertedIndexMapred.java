package inverted;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
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
			
			articleName = articleId.toString(); 
			line = indices.toString();
			comma = line.indexOf(',');
			lemma = new Text(line.substring(1, comma));
			count = Integer.parseInt(line.substring(comma + 1, line.length() - 1));
			
			index = new StringInteger(articleName, count);
			
			context.write(lemma, index);
			
		}
	}

	public static class InvertedIndexReducer extends
			Reducer<Text, StringInteger, Text, StringIntegerList> {
		StringIntegerList indexWritable;
		
		@Override
		public void reduce(Text lemma, Iterable<StringInteger> articlesAndFreqs, Context context)
				throws IOException, InterruptedException {
			// TODO: You should implement inverted index reducer here
			
			ArrayList<StringInteger> indexList = new ArrayList<StringInteger>();
			for (StringInteger articleAndFreq : articlesAndFreqs){
				indexList.add(articleAndFreq);
			}
			
			indexWritable = new StringIntegerList(indexList);
			
			context.write(lemma, indexWritable);
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO: you should implement the Job Configuration and Job call
		// here
		//GenericOptionsParser gop = new GenericOptionsParser(String[] args);

		Configuration conf = new Configuration();
		GenericOptionsParser gop = new GenericOptionsParser(conf, args);
		String[] otherArgs = gop.getRemainingArgs();
		
		Job job = Job.getInstance(conf, "inverted index");
		
		job.setJarByClass(InvertedIndexMapred.class);
		job.setMapperClass(InvertedIndexMapper.class);
		job.setReducerClass(InvertedIndexReducer.class);
		
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
