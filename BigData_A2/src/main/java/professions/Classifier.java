package professions;

import java.io.IOException;
import java.io.StringReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.mahout.classifier.naivebayes.NaiveBayesModel;
import org.apache.mahout.classifier.naivebayes.StandardNaiveBayesClassifier;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;

import util.StringIntegerList;

public class Classifier {
	public final static String MODEL_PATH_CONF = "modelPath";
	public final static String DICTIONARY_PATH_CONF = "dictionaryPath";
	public final static String DOCUMENT_FREQUENCY_PATH_CONF = "documentFrequencyPath";
	
	private static StandardNaiveBayesClassifier classifier;
	private static Map<String, Integer> dictionary;

	public Classifier(Configuration configuration) throws IOException {
		String modelPath = configuration.getStrings(MODEL_PATH_CONF)[0];
		String dictionaryPath = configuration.getStrings(DICTIONARY_PATH_CONF)[0];
		
		dictionary = readDictionary(configuration, new Path(dictionaryPath));
		
		NaiveBayesModel model = NaiveBayesModel.materialize(new Path(modelPath), configuration);
		
		classifier = new StandardNaiveBayesClassifier(model);
	}
	
	public String[] classify(String articleId){
		//then magic happens and the articleID gets mapped to a vector?????
		//Vector articleVector = new Vector(articleId); ???
		
		// With the classifier, we get one score for each profession
		// The three professions with the highest scores are what this article should be associated to
		Vector resultVector = classifier.classifyFull(articleVector);
		HashMap<String, Double> topScores;
		topScores.put("none 1", 0.0);
		topScores.put("none 2", 0.0);
		topScores.put("none 3", 0.0);
		int bestCategoryId = -1;
		double minimum;
		
		for(Element element: resultVector.all()) {
			
			String categoryId = element.index();
			double score = element.get();
			
			if ((minimum = Collections.min(topScores.values())) < score){
				topScores.put(categoryId, score);
				topScores.values().remove(minimum);
			}
		
		return topScores.keySet().toArray(new String[3]);
		}
	}
	
	private static Map<String, Integer> readDictionary(Configuration conf, Path dictionaryPath) {
		Map<String, Integer> dictionnary = new HashMap<String, Integer>();
		for (Pair<Text, IntWritable> pair : new SequenceFileIterable<Text, IntWritable>(dictionaryPath, true, conf)) {
			dictionnary.put(pair.getFirst().toString(), pair.getSecond().get());
		}
		return dictionnary;
	}
}
