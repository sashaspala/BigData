package articles;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
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
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umd.cloud9.collection.wikipedia.WikipediaPage;
//import edu.umd.cloud9.collection.wikipedia.WikipediaPageInputFormat;
import util.WikipediaPageInputFormat;
//import mapreduce.NameCount.Map;

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
	 * 		Text (name)		Text (xml page)
	 * @author Tuan
	 *
	 */
	//@formatter:on
	public static class GetArticlesMapper extends Mapper<LongWritable, WikipediaPage, Text, Text> {
		public static Set<String> peopleArticlesTitles = new HashSet<String>();
		private final static IntWritable one = new IntWritable(1);	//is this just here out of habit?
		private Text fullName = new Text();							//don't know what this is either
		//private HashMap<String, ArrayList<String>> names;			//why?
		private HashSet<String> names;

		@Override
		protected void setup(Mapper<LongWritable, WikipediaPage, Text, Text>.Context context)
				throws IOException, InterruptedException {
//		protected void setup(Context context) throws IOException, InterruptedException {
			// TODO: You should implement people articles load from
			// DistributedCache here
			
			//System.out.println("entered setup");
			
			super.setup(context);
			
			URI[] files = context.getCacheFiles();
			BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(files[0].getPath()), "UTF-8"));
			names = new HashSet<String>();
			
			String line = reader.readLine();
			while(line != null){
				names.add(line);
				line = reader.readLine();				
			}
			reader.close();
		}

		@Override
		public void map(LongWritable offset, WikipediaPage inputPage, Context context)
				throws IOException, InterruptedException {
			// TODO: You should implement getting article mapper here
			
			String title = inputPage.getTitle();
			String xml = inputPage.getRawXML();
			if(names.contains(title)){
				context.write(new Text(title), new Text(xml));
			}
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		// TODO: you should implement the Job Configuration and Job call
		// here
		Configuration conf = new Configuration();
		GenericOptionsParser gop = new GenericOptionsParser(conf, args);
		String[] otherArgs = gop.getRemainingArgs();
		Job job = Job.getInstance(conf, "get articles");
		
//		job.addCacheFile(new Path(args[2]).toUri());	//people.txt is 3rd argument
//		ClassLoader cl = GetArticlesMapred.class.getClassLoader();
//		job.addCacheFile(cl.getResource("people.txt").toURI());		//just going to include this as a resource like in assignment 1
		job.addCacheFile(new URI("people.txt"));	//this probably isn't the most efficient solution, but I got it to work on the cluster -- Meghan
		
		job.setJarByClass(GetArticlesMapred.class);
		job.setMapperClass(GetArticlesMapper.class);
		job.setInputFormatClass(WikipediaPageInputFormat.class);	//why didn't this work? -- because I hadn't imported it =P
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		//job.setCombinerClass(GetArticlesCombiner.class);
		//job.setReducerClass(GetArticlesReducer.class);
		//job.setOutputKeyClass(Text.class);
		//job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		try{
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
		catch (ClassNotFoundException e){
			e.printStackTrace();
		}
		catch (InterruptedException e){
			e.printStackTrace();
		}

	}
}
