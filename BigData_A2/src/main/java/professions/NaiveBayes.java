package professions;

import java.io.IOException;
import professions.Classifier;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import util.StringIntegerList;

public class NaiveBayes {
	public static class TestMapper extends Mapper<Text, StringIntegerList, Text, ArrayWritable> {
		private static Text outputKey = new Text();
		private static ArrayWritable outputValue = null;
		private static Classifier classifier;

		@Override
		protected void setup(Context context) throws IOException {
			initClassifier(context);
		}

		private static void initClassifier(Context context) throws IOException {
			if (classifier == null) {
				synchronized (TestMapper.class) {
					if (classifier == null) {
						classifier = new Classifier(context.getConfiguration());
					}
				}
			}
		}

		public void TestMap(Text articleId, StringIntegerList lemmas, Context context) throws IOException, InterruptedException {
			outputKey.set(articleId);
			String[] bestCategoryIds = classifier.classify(articleId.toString());
			outputValue = new ArrayWritable(bestCategoryIds);
			context.write(outputKey, outputValue);
		}
	}

	public static void main(String[] args){
		if (args.length < 5) {
			System.out.println("Arguments: [model] [dictionary] [document frequency] [input file] [output directory]");
			return;
		}
		String modelPath = args[0];
		String dictionaryPath = args[1];
		String documentFrequencyPath = args[2];
		String inputPath = args[3];
		String outputPath = args[4];
	
		Configuration conf = new Configuration();
	
		conf.setStrings(Classifier.MODEL_PATH_CONF, modelPath);
		conf.setStrings(Classifier.DICTIONARY_PATH_CONF, dictionaryPath);
		conf.setStrings(Classifier.DOCUMENT_FREQUENCY_PATH_CONF, documentFrequencyPath);
	
		// do not create a new jvm for each task
		conf.setLong("mapred.job.reuse.jvm.num.tasks", -1);
	
		Job job;
		try {
			job = Job.getInstance(conf, "classifier");
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(ArrayWritable.class);
			job.setMapperClass(TestMapper.class);
		
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
		
			FileInputFormat.addInputPath(job, new Path(inputPath));
			FileOutputFormat.setOutputPath(job, new Path(outputPath));
			
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
		catch (ClassNotFoundException e){
			e.printStackTrace();
		}
		catch (InterruptedException e){
			e.printStackTrace();
		}
		catch (IOException e){
			e.printStackTrace();
		}
	}
	
}
