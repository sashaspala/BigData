package professions;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.jar.JarFile;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import mapreduce.ProcessPeople;
import mapreduce.ProcessPeople.IntSumCombiner;
import mapreduce.ProcessPeople.IntSumReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ProfessionIndexMapred {
	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
		private final static IntWritable one = new IntWritable(1);
		final String PROFESSIONS_FILE = "professions.txt";
		HashMap<String, Vector<String>> nameToProfession;
		static String DELIM = ":";

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// Tokenize the input with characters in DELIM, without returning
			// any delimit character
			StringTokenizer itr = new StringTokenizer(value.toString(), ",", false);
			HashSet<String> tokens = new HashSet<String>();
			// Store all tokens in a hashset to avoid duplicate check
			if (itr.hasMoreTokens()) {
				String token = itr.nextToken();
				tokens.add(token);
			}
			
			for (String token : tokens) {
				Pattern pat = Pattern.compile("<(\\w+),\\d+>");
				Matcher mat = pat.matcher(token);
				while (mat.find()){
					String matchedToken = mat.group(1);
					if (nameToProfession.containsKey(matchedToken)) {
						for (String profession : nameToProfession.get(matchedToken)) {
							if (value.toString().contains(profession)) {
								context.write(new Text(matchedToken), new Text(profession));
							}
						}
					}
				}
			}
		}

		public void setup(Context context) throws IOException, InterruptedException {
			ClassLoader cl = ProcessPeople.class.getClassLoader();

			String fileUrl = cl.getResource(PROFESSIONS_FILE).getFile();

			// Get jar path
			String jarUrl = fileUrl.substring(5, fileUrl.length() - PROFESSIONS_FILE.length() - 2);

			JarFile jf = new JarFile(new File(jarUrl));
			Scanner sc;
			// Scan the people.txt file inside jar
			sc = new Scanner(jf.getInputStream(jf.getEntry(PROFESSIONS_FILE)));

			nameToProfession = new HashMap<String, Vector<String>>();

			// Indexing people's name using their first name
			while (sc.hasNextLine()) {
				String line = sc.nextLine();
				StringTokenizer itr = new StringTokenizer(line, DELIM);
				if (itr.hasMoreTokens()) {
					String firstname = itr.nextToken();
					String profession = itr.nextToken();
					if (!nameToProfession.containsKey(firstname)) {
						nameToProfession.put(firstname, new Vector<String>());
					}
					if (!nameToProfession.get(firstname).contains(profession))
						nameToProfession.get(firstname).add(profession);
				}
			}
			
			sc.close();
			jf.close();

		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "process professions");
		job.setJarByClass(ProcessPeople.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumCombiner.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		for (int i = 0; i < args.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(args[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
