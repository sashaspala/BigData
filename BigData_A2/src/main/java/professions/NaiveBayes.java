package professions;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Map;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.jar.JarFile;
import java.util.regex.Pattern;

import professions.Classifier;
import mapreduce.ProcessPeople;
import mapreduce.ProcessPeople.IntSumCombiner;
import mapreduce.ProcessPeople.IntSumReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.mahout.classifier.naivebayes.BayesUtils;
import org.apache.mahout.classifier.naivebayes.NaiveBayesModel;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.math.VectorWritable;


public class NaiveBayes {
	public class BayesTestMapper extends Mapper<Text, VectorWritable, Text, Text> {
		final String PROFESSIONS_FILE = "professions.txt";
		static final String DELIM = ":";
		  private final Pattern TAB = Pattern.compile("\t");
		  private Classifier classifier;
		  
		  private Configuration conf;
		  private Map<Integer, String> labels;
		  @Override
		  protected void setup(Context context) throws IOException, InterruptedException {
		    
			  super.setup(context);
			 conf = context.getConfiguration();
		    //Path[] paths = HadoopUtil.getCachedFiles(conf);
		    //Path modelPath = paths[0];
		    Path modelPath = new Path(conf.get("modelPath"));
		    Path labelPath = new Path(conf.get("labelPath"));
		    
		    NaiveBayesModel model = NaiveBayesModel.materialize(modelPath, conf);
		    labels = BayesUtils.readLabelIndex(conf, labelPath);
		    classifier = new Classifier(model);
		  }

		  @Override
		  protected void map(Text key, VectorWritable value, Context context) throws IOException, InterruptedException {
		    Integer[] result = classifier.classify(value.get());
		    String finalCategories = "";
		    for(int i = 0; i < result.length; i++){
		    	if(labels.containsKey(result[i])){
		    		//check validity
		    		if(i == 0){
		    			finalCategories = finalCategories + ", " + labels.get(result[i]);
		    		}
		    		else{
		    			finalCategories = labels.get(result[i]);
		    		}
		    	}
		    	else{
		    		if(i == 0){
		    			finalCategories = "Label not found";
		    		}
		    		else{
		    			finalCategories = finalCategories + ", " + "Label not found";
		    		}
		    	}
		    }
		   
		    //the key is the expected value
		    context.write(new Text(TAB.split(key.toString())[0]), new Text(finalCategories));
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

	public static void main(String[] args) throws Exception{
		if (args.length < 4) {
			System.out.println("Arguments: [model] [input file] [output directory] [label index]");
			return;
		}
		String modelPath = args[0];
		String inputPath = args[1];
		String outputPath = args[2];
		String labelPath = args[3];
	
		Configuration conf = new Configuration();
		conf.set("mapred.job.queue.name", "hadoop02");
		
	
		// do not create a new jvm for each task
		conf.setLong("mapred.job.reuse.jvm.num.tasks", -1);
		

		//train model
		//TrainNaiveBayesJob trainNaiveBayes = new TrainNaiveBayesJob();
		//trainNaiveBayes.setConf(conf);
		//trainNaiveBayes.run(new String[] {modelPath, inputPath, tempPath});
		//NaiveBayesModel naiveBayesModel = NaiveBayesModel.materialize(new Path(outputPath), conf);
		
		//Classifier classifier = new Classifier(naiveBayesModel);

		
		conf.setStrings(Classifier.MODEL_PATH_CONF, modelPath);
		conf.setStrings(Classifier.LABEL_PATH_CONF, labelPath);
		//test model in distributed fashion
		Job job;
		try {
			job = Job.getInstance(conf, "classifier");
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(ArrayWritable.class);
			job.setMapperClass(BayesTestMapper.class);
			job.setCombinerClass(IntSumCombiner.class);
			job.setReducerClass(IntSumReducer.class);
		
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
