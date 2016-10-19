package lemma;

import java.io.IOException;
import java.io.FileNotFoundException

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import util.StringIntegerList;
import edu.umd.cloud9.collection.wikipedia.WikipediaPage;

import java.util.HashMap;
import java.util.Map;
import java.util.List;

import lemma.Tokenizer;

/**
 * Author: Elizabeth
 * Creates an index of lemma frequencies for each article.
 */
public class LemmaIndexMapred {
	public static class LemmaIndexMapper extends Mapper<LongWritable, WikipediaPage, Text, StringIntegerList> {

		@Override
		public void map(LongWritable offset, WikipediaPage page, Context context) throws IOException,
				InterruptedException {
			// Get content of page - comes from WikiPage from GetArticlesMapred
			Text title = new Text(page.getTitle());
			String text = page.getWikiMarkup();
			// Tokenize
			Tokenizer tokenizer = new Tokenizer();
			List<String> lemmas = tokenizer.tokenize(text);
			// Count lemmas
			Map<String, Integer> freqs = new HashMap<String, Integer>();
			for (String lemma : lemmas){
				if (freqs.containsKey(lemma)) {
					freqs.put(lemma, freqs.get(lemma) + 1);
				} else {
					freqs.put(lemma, 1);
				}
			}
			StringIntegerList list = new StringIntegerList(freqs);
			// Write output
			context.write(title, list);
		}
	}

	public static void main(String[] args) throws FileNotFoundException, IOException, ClassNotFoundException, InterruptedException{

		// Load Class with ClassLoader
		ClassLoader cl = LemmaIndexMapred.class.getClassLoader();

		// Get configuration to set up job
		Configuration conf = new Configuration();

		// Get args
		GenericOptionsParser gop = new GenericOptionsParser(conf, args);
		String[] otherArgs = gop.getRemainingArgs();

		// Get job and set variables
		Job job = Job.getInstance(conf, "Lemma Index");
		job.setJarByClass(LemmaIndexMapred.class);
		job.setMapperClass(LemmaIndexMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(StringIntegerList.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// Submit job and wait until it's completed to end program
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
