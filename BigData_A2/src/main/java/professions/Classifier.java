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
	public final static String LABEL_PATH_CONF = "labelPath";
	public final static String PROF_PATH_CONF = "profPath";
	
	private static StandardNaiveBayesClassifier classifier;

	public Classifier(NaiveBayesModel model) throws IOException {
		classifier = new StandardNaiveBayesClassifier(model);
	}
	
	public Integer[] classify(Vector articleVector){
		
		// With the classifier, we get one score for each profession
		// The three professions with the highest scores are what this article should be associated to
		Vector resultVector = classifier.classifyFull(articleVector);
		HashMap<Integer, Double> topScores = new HashMap<Integer, Double>();
		topScores.put(-1, 0.0);
		topScores.put(-2, 0.0);
		topScores.put(-3, 0.0);
		double minimum;
		
		for(Element element: resultVector.all()) {
			
			Integer categoryId = element.index();
			double score = element.get();
			
			if ((minimum = Collections.min(topScores.values())) < score){
				topScores.put(categoryId, score);
				topScores.values().remove(minimum);
			}
		}
		
		return topScores.keySet().toArray(new Integer[3]);

	}	
		
}
