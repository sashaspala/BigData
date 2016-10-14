/**
 * Author: Schuyler Rank
 * Email: srank@brandeis.edu
 * Date: 10/2/16
 * Class: COSI 129a
 * Assignment: 1 - Working with Hadoop and MapReduce
 * Description: This is a MapReduce implementation of a program that takes an input file and counts all occurrences of 
 * 				names in the packaged people.txt file. It outputs the names in the format "COUNT NAME".
 */

package mapreduce;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NameCount {
	public static class NameFreqsMapper extends Mapper<Object, Text, Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		private HashSet<String> names;
//		private int longestLength;	//my mappers keep failing with GC overhead limit errors when I try to calculate this
		private static int LONGEST_LENGTH = 14;	//so I'm just using what I know to be the largest number of space-separated
												//tokens in a single line in the bundled people.txt
		
		@Override
		protected void setup(Context context) throws IOException{
			//FIRST, build the set of names that we're looking for
			names = new HashSet<String>();
			
			URI[] files = context.getCacheFiles();
			BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(files[0].getPath()), "UTF-8"));

			String line = reader.readLine();
			while(line != null){
//				if(line.split(" +").length > longestLength)		//finds the longest name length but my mappers crash
//					longestLength = line.split(" +").length;	//with GC overhead limit errors using it
				names.add(line);	//there are about 60k repeats but we can't disambiguate them and sets don't care
				line = reader.readLine();				
			}
			reader.close();
		}
		
		/**
		 * This looks through the text Value and finds matches on the names created in the setup method.
		 * When it finds a match, it writes the name found and 1 to the context.
		 * @param key not used
		 * @param value the text to search
		 * @param context where we keep track of the found names
		 */
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			//SECOND, search the text for those names
			String[] tokens = value.toString().split(" +");
			int iMax = tokens.length;	//maximum index so we don't go off the end of the array
			
			//check for names that start with the first token, then for names that start with the second token, etc.
			for(int i=0; i<iMax; i++){
				//how many tokens can we search before going off the line or beyond the longest name?
				int searchDistance = LONGEST_LENGTH < (iMax-i) ? LONGEST_LENGTH : iMax-i; 
				
				String growing = tokens[i];	//this is our start token
				
				if(growing.length() == 0)	//this happens and it's a waste of time
					continue;
				
				//check if it's a name all by itself
				if(names.contains(growing))
					context.write(new Text(growing), one);
				
				//remove anything sticking to the beginning of this token like < or ( or "
				while(growing.length() > 0 && !Character.isLetterOrDigit(growing.charAt(0))){
					growing = growing.substring(1);
					
					//check it every time we try to fix it
					if(names.contains(growing)){
						context.write(new Text(growing), one);
					}
				}
				
				//remove anything sticking to the end (but don't take a token that's been shaved at the end then add to it)
				String temp = new String(growing);
				while(growing.length() > 1 
						&& (growing.endsWith("'s") || !Character.isLetterOrDigit(growing.charAt(growing.length()-1)))){
					growing = growing.substring(0, growing.length()-1);
					
					//check it every time we try to fix it
					if(names.contains(growing)){
						context.write(new Text(growing), one);
					};
				}
				growing = temp;
				
				//we may have just emptied the current start token; if so, move on to the next start token
				//also, while some names start with ( or ' or even numbers, NONE start with a lowercase letter
				if(growing.length() == 0 || Character.isLowerCase(growing.charAt(0)))
					continue;
				
				//from the start token, check start + next, then start + 2 next, and so on 
				//until we hit the end of the line or the limit of name length
				for(int j=1; j<searchDistance; j++){
					growing = growing + " " + tokens[i+j];
					
					//check for nameness after adding a token
					if(names.contains(growing))
						context.write(new Text(growing), one);					
					
					//take off anything weird at the end of the newly-added token (but don't save those changes, again
					//because we might add to this later and we don't want to do so after altering something in the middle)
					temp = new String(growing);
					while(growing.length() > 1 && (growing.endsWith("'s") || !Character.isLetterOrDigit(growing.charAt(growing.length()-1)))){
						growing = growing.substring(0, growing.length()-1);
						
						//check it every time we try to fix it
						if(names.contains(growing))
							context.write(new Text(growing), one);
					}
					growing = temp;
				}
			}
		}		
	}
	
	/**
	 * Since the mapper just emits "NAME 1", we can make the reducer's job easier while waiting for the mappers to 
	 * finish by doing some intermediate summing, so 5 occurrences of "NAME 1" become "NAME 5". It needs to have the
	 * same and same order of input and output keys and values because the reducer can't know if its input came from
	 * a mapper or a combiner.
	 */
	public static class NameFreqsCombiner extends Reducer<Text, IntWritable, Text, IntWritable>{		
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			int sum = 0;
			for(IntWritable val : values){
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));			
		}
	}
	
	/**
	 * This finishes the NameFreqs job by summing the values from the mappers and combiners into one final value per key.
	 */
	public static class NameFreqsReducer extends Reducer<Text, IntWritable, IntWritable, Text>{		
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			int sum = 0;
			for(IntWritable val : values){
				sum += val.get();
			}
			context.write(new IntWritable(sum), key);			
		}
	}
	
	/**
	 * @param args input directory+, output directory
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 * @throws URISyntaxException 
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "name freqs");
		
		//classloader stuff, get the file of names
		ClassLoader cl = NameCount.class.getClassLoader();
		job.addCacheFile(cl.getResource("people.txt").toURI());
		
		job.setJarByClass(NameCount.class);
		job.setMapperClass(NameFreqsMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setCombinerClass(NameFreqsCombiner.class);
		job.setReducerClass(NameFreqsReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		for(int i=0; i<args.length-1; ++i){
			FileInputFormat.addInputPath(job, new Path(args[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(args[args.length-1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}