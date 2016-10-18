package mapreduce;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.jar.JarFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ProcessPeople {
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		final String PEOPLE_FILE = "people.txt";
		HashMap<String, Vector<String>> firstNameToFullNames;
		static String DELIM = " ,.:;'\"()[]{}<>";

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// Tokenize the input with characters in DELIM, without returning
			// any delimit character
			StringTokenizer itr = new StringTokenizer(value.toString(), DELIM, false);
			HashSet<String> tokens = new HashSet<String>();

			// Store all tokens in a hashset to avoid duplicate check
			if (itr.hasMoreTokens()) {
				String token = itr.nextToken();
				tokens.add(token);
			}

			for (String token : tokens) {
				if (firstNameToFullNames.containsKey(token)) {
					for (String fullName : firstNameToFullNames.get(token)) {
						if (value.toString().contains(fullName)) {
							// The next character after the full name should be
							// a 'space' character
							int nextIndex = value.toString().indexOf(fullName) + fullName.length();
							if (nextIndex >= value.toString().length()
									|| DELIM.contains(value.toString().substring(nextIndex, nextIndex + 1))) {
								context.write(new Text(fullName), one);
							}
						}
					}
				}
			}
		}

		public void setup(Context context) throws IOException, InterruptedException {
			ClassLoader cl = ProcessPeople.class.getClassLoader();

			String fileUrl = cl.getResource(PEOPLE_FILE).getFile();

			// Get jar path
			String jarUrl = fileUrl.substring(5, fileUrl.length() - PEOPLE_FILE.length() - 2);

			JarFile jf = new JarFile(new File(jarUrl));
			Scanner sc;
			// Scan the people.txt file inside jar
			sc = new Scanner(jf.getInputStream(jf.getEntry(PEOPLE_FILE)));

			firstNameToFullNames = new HashMap<String, Vector<String>>();

			// Indexing people's name using their first name
			while (sc.hasNextLine()) {
				String name = sc.nextLine();
				StringTokenizer itr = new StringTokenizer(name);
				if (itr.hasMoreTokens()) {
					String firstname = itr.nextToken();
					if (!firstNameToFullNames.containsKey(firstname)) {
						firstNameToFullNames.put(firstname, new Vector<String>());
					}
					
					if (!firstNameToFullNames.get(firstname).contains(name))
						firstNameToFullNames.get(firstname).add(name);
				}
			}
			
			sc.close();
			jf.close();

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

		Job job = Job.getInstance(conf, "process people");
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
