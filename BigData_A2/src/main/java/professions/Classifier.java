package professions;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.classifier.naivebayes.NaiveBayesModel;
import org.apache.mahout.classifier.naivebayes.StandardNaiveBayesClassifier;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;


public class Classifier {
	public final static String MODEL_PATH_CONF = "modelPath";
	
	private static StandardNaiveBayesClassifier classifier;

	public Classifier(NaiveBayesModel model) throws IOException {
		classifier = new StandardNaiveBayesClassifier(model);
	}
	
	public String[] classify(Vector articleVector){
		
		// With the classifier, we get one score for each profession
		// The three professions with the highest scores are what this article should be associated to
		Vector resultVector = classifier.classifyFull(articleVector);
		HashMap<String, Double> topScores = new HashMap<String, Double>();
		topScores.put("error1", 0.0);
		topScores.put("error2", 0.0);
		topScores.put("error3", 0.0);
		double minimum;
		
		for(Element element: resultVector.all()) {
			
			String categoryId = Integer.toString(element.index());
			double score = element.get();
			
			if ((minimum = Collections.min(topScores.values())) < score){
				topScores.put(categoryId, score);
				topScores.values().remove(minimum);
			}
		}
		return topScores.keySet().toArray(new String[3]);

	}	
		
}
