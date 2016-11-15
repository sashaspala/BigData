package professions;

import java.io.IOException;
import java.util.regex.Pattern;

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
import org.apache.mahout.classifier.naivebayes.AbstractNaiveBayesClassifier;
import org.apache.mahout.classifier.naivebayes.NaiveBayesModel;
import org.apache.mahout.classifier.naivebayes.StandardNaiveBayesClassifier;
import org.apache.mahout.classifier.naivebayes.test.TestNaiveBayesDriver;
import org.apache.mahout.classifier.naivebayes.training.TrainNaiveBayesJob;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import util.StringIntegerList;

public class NaiveBayes {
	public class BayesTestMapper extends Mapper<Text, VectorWritable, Text, ArrayWritable> {
		  private final Pattern TAB = Pattern.compile("\t");
		  private Classifier classifier;

		  @Override
		  protected void setup(Context context) throws IOException, InterruptedException {
		    
			  super.setup(context);
		    Configuration conf = context.getConfiguration();
		    Path modelPath = HadoopUtil.getSingleCachedFile(conf);
		    
		    NaiveBayesModel model = NaiveBayesModel.materialize(modelPath, conf);
		    
		    classifier = new Classifier(model);
		  }

		  @Override
		  protected void map(Text key, VectorWritable value, Context context) throws IOException, InterruptedException {
		    String[] result = classifier.classify(value.get());
		    //the key is the expected value
		    context.write(new Text(TAB.split(key.toString())[0]), new ArrayWritable(result));
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
	
		
	
		// do not create a new jvm for each task
		conf.setLong("mapred.job.reuse.jvm.num.tasks", -1);
		

		//train model
		//TrainNaiveBayesJob trainNaiveBayes = new TrainNaiveBayesJob();
		//trainNaiveBayes.setConf(conf);
		//trainNaiveBayes.run(new String[] {modelPath, inputPath, tempPath});
		//NaiveBayesModel naiveBayesModel = NaiveBayesModel.materialize(new Path(outputPath), conf);
		
		//Classifier classifier = new Classifier(naiveBayesModel);

		
		conf.setStrings(Classifier.MODEL_PATH_CONF, modelPath);
		//test model in distributed fashion
		Job job;
		try {
			job = Job.getInstance(conf, "classifier");
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(ArrayWritable.class);
			job.setMapperClass(BayesTestMapper.class);
		
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
