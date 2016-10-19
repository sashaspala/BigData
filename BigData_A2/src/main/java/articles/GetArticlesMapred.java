package articles;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;
import java.util.jar.JarFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import edu.umd.cloud9.collection.wikipedia.WikipediaPage;
import edu.umd.cloud9.collection.wikipedia.WikipediaPageInputFormat;
//import mapreduce.NameCount.Map;
import mapreduce.NameCount;
import mapreduce.NameCount.NameFreqsCombiner;
import mapreduce.NameCount.NameFreqsMapper;
import mapreduce.NameCount.NameFreqsReducer;

/**
 * This class is used for Section A of assignment 1. You are supposed to
 * implement a main method that has first argument to be the dump wikipedia
 * input filename , and second argument being an output filename that only
 * contains articles of people as mentioned in the people auxiliary file.
 */
public class GetArticlesMapred {

	//@formatter:off
	/**
	 * Input:
	 * 		Page offset 	WikipediaPage
	 * Output
	 * 		Page offset 	WikipediaPage
	 * @author Tuan
	 *
	 */
	//@formatter:on
	public static class GetArticlesMapper extends Mapper<LongWritable, WikipediaPage, LongWritable, WikipediaPage> {
		public static Set<String> peopleArticlesTitles = new HashSet<String>();
		private final static IntWritable one = new IntWritable(1);	//is this just here out of habit?
		private Text fullName = new Text();
		//private HashMap<String, ArrayList<String>> names;
		private ArrayList<String> names;

		@Override
		protected void setup(Mapper<LongWritable, WikipediaPage, LongWritable, WikipediaPage>.Context context)
				throws IOException, InterruptedException {
//		protected void setup(Context context) throws IOException, InterruptedException {
			// TODO: You should implement people articles load from
			// DistributedCache here
			
			System.out.println("entered setup");
			
			super.setup(context);
			
			URI[] files = context.getCacheFiles();
			File wikiFile = new File(files[0].getPath());	
//			Scanner scan = new Scanner(wikiFile);
			BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(files[0].getPath()), "UTF-8"));
			//names = new HashMap<String, ArrayList<String>>(1000000);	//expecting ~700k keys, 0.75 default load factor
			names = new ArrayList<String>();
			
			String line = reader.readLine();
			while(line != null){
				//names.put(line, new ArrayList<String>());	//there are about 60k repeats but who cares
				names.add(line);	//it's unimportant that there are some repeats
				line = reader.readLine();				
			}
			reader.close();
			System.out.println("exiting setup");
		}

		@Override
		public void map(LongWritable offset, WikipediaPage inputPage, Context context)
				throws IOException, InterruptedException {
			// TODO: You should implement getting article mapper here
			System.out.println("entering map");
			
//			if(names.containsKey(inputPage.getTitle())){
//				System.out.println("found a name:" + inputPage.getTitle());
//				ArrayList<String> articles = names.get(inputPage.getTitle());
//				articles.add(inputPage.getContent());
//				names.put(inputPage.getTitle(), articles);
//			}
//			
//			for(String name : names.keySet()){
//				System.out.println("writing some stuff");
//				ArrayList<String> articles = names.get(name);
//				for(int i=0; i<articles.size(); i++)
//					context.write(new Text(name), new Text(articles.get(i)));
//			}
			
			System.out.println("exiting map");
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		// TODO: you should implement the Job Configuration and Job call
		// here
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "get articles");
		
//		job.addCacheFile(new Path(args[2]).toUri());	//people.txt is 3rd argument
		//couldn't get the above to work "URI is not absolute" or some bs
		ClassLoader cl = NameCount.class.getClassLoader();
		job.addCacheFile(cl.getResource("people.txt").toURI());
		
		job.setJarByClass(GetArticlesMapred.class);
		job.setMapperClass(GetArticlesMapper.class);
		//job.setInputFormatClass(WikipediaPageInputFormat.class);	//why doesn't this work?
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(WikipediaPage.class);
		//job.setCombinerClass(GetArticlesCombiner.class);
		//job.setReducerClass(GetArticlesReducer.class);
		//job.setOutputKeyClass(Text.class);
		//job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
