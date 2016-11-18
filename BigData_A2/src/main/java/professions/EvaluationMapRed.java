package professions;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.jar.JarFile;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import mapreduce.ProcessPeople;
import mapreduce.ProcessPeople.IntSumCombiner;
import mapreduce.ProcessPeople.IntSumReducer;
import mapreduce.ProcessPeople.TokenizerMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class EvaluationMapRed {

	public static class EvalMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
		private final static IntWritable SUCCESS = new IntWritable(1);
		private final static IntWritable FAIL = new IntWritable(0);
		private final static IntWritable one = new IntWritable(1);
		final String PROFESSIONS_FILE = "professions.txt";
		HashMap<String, ArrayList<String>> nameToProfession;
		static String DELIM = ":";

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// Tokenize the input with characters in DELIM, without returning
			// any delimit character
			StringTokenizer lineItr = new StringTokenizer(value.toString(), "\n");
			while(lineItr.hasMoreTokens()){
				String line = lineItr.nextToken();
				StringTokenizer entitiesItr = new StringTokenizer(line, "\t");
				String name = entitiesItr.nextToken().trim();
				String professions = entitiesItr.nextToken().trim();
				
				StringTokenizer profItr = new StringTokenizer(professions, ",");
				boolean valid = false;
				if (nameToProfession.containsKey(name)){
					while(profItr.hasMoreTokens()){
						if(nameToProfession.get(name).contains(profItr.nextToken())){
							valid = true;
							break;
						}
						
					}
					if(valid){
						context.write(SUCCESS, one);
					}
					else{
						context.write(FAIL, one);
					}
				}
				
			}
			
			
		}

		public void setup(Context context) throws IOException, InterruptedException {
			mapProfessions();
		}
		
		protected void mapProfessions(){
			  ClassLoader cl = NaiveBayes.class.getClassLoader();
			  String fileUrl = cl.getResource(PROFESSIONS_FILE).getFile();
			  String jarUrl = fileUrl.substring(5, fileUrl.length() - PROFESSIONS_FILE.length() - 2);
			  JarFile jf;
			  Scanner sc;
			try {
				jf = new JarFile(new File(jarUrl));
				sc = new Scanner(jf.getInputStream(jf.getEntry(PROFESSIONS_FILE)));
				 while (sc.hasNextLine()) {
						String line = sc.nextLine();
						StringTokenizer itr = new StringTokenizer(line, DELIM);
						if (itr.hasMoreTokens()) {
							String firstname = itr.nextToken();
							String professions = itr.nextToken();
							StringTokenizer profItr = new StringTokenizer(professions, ",");
							ArrayList<String> profArray = new ArrayList<String>();
							while(profItr.hasMoreTokens()){
								profArray.add(profItr.nextToken().trim());
							}
							if (!nameToProfession.containsKey(firstname)) {
								nameToProfession.put(firstname, profArray);
							}
							else{
								ArrayList<String> prevArray = nameToProfession.get(firstname);
								prevArray.addAll(profArray);
								nameToProfession.put(firstname, prevArray);
							}
								
						}
					}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	
		}
	}
	
	public static class IntSumCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, IntWritable, Text> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(result, key);
		}
	}
	
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		conf.set("mapred.job.queue.name", "hadoop02");

		Job job = Job.getInstance(conf, "evaluate naive bayes");
		job.setJarByClass(EvaluationMapRed.class);
		job.setMapperClass(EvalMapper.class);
		job.setCombinerClass(IntSumCombiner.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		for (int i = 0; i < args.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(args[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
