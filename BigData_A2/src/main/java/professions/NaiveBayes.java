package professions;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;
import org.apache.mahout.classifier.naivebayes.BayesUtils;
import org.apache.mahout.classifier.naivebayes.NaiveBayesModel;
import org.apache.mahout.classifier.naivebayes.StandardNaiveBayesClassifier;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.vectorizer.TFIDF;

import com.google.common.collect.Multiset;

//this is far from done, but I'm tired & going to sleep.

public class NaiveBayes {
	
	public static Map<String, Integer> readDict(Configuration conf, Path dictPath) {
		//creates a dictionary of 
		Map<String, Integer> dict = new HashMap<String, Integer>();
		for (Pair<Text, IntWritable> pair : new SequenceFileIterable<Text, IntWritable>(dictPath, true, conf)) {
			dict.put(pair.getFirst().toString(), pair.getSecond().get());
		}
		return dict;
	}
	
	public static Map<Integer, Long> readDocFreq(Configuration conf, Path docFreqPath){
		
		Map<Integer, Long> docFreq = new HashMap<Integer, Long>();
		for (Pair<IntWritable, LongWritable> pair : new SequenceFileIterable<IntWritable, LongWritable>(docFreqPath, true, conf)) {
			docFreq.put(pair.getFirst().get(), pair.getSecond().get());
		}
		return docFreq;
	}
	
	public static void main(String[] args) throws Exception {
		if (args.length < 4) {
			System.out.println("Arguments: [model] [label index] [dictionary] [document frequency] [text document path]");
			return;
		}
		String modelPath = args[0];
		String labelIndexPath = args[1];
		String dictPath = args[2];
		String docFreqPath = args[3];
		String articlesPath = args[4];
		
		Configuration conf = new Configuration();

		// model is a matrix (wordId, labelID) => probability score
		NaiveBayesModel model = NaiveBayesModel.materialize(new Path(modelPath), conf);
		
		StandardNaiveBayesClassifier classifier = new StandardNaiveBayesClassifier(model);

		// labels is a map label => classId
		Map<Integer, String> labels = BayesUtils.readLabelIndex(conf, new Path(labelIndexPath));
		Map<String, Integer> dictionary = readDict(conf, new Path(dictPath));
		Map<Integer, Long> docFreq = readDocFreq(conf, new Path(docFreqPath));

		int labelCount = labels.size();
		int docCount = docFreq.get(-1).intValue();
		
		System.out.println("Number of documents in training set: " + docCount);
		
		while(true) {
			
			
			
			// create vector wordId => weight using tfidf
			Vector vector = new RandomAccessSparseVector(10000);
			TFIDF tfidf = new TFIDF();
			for (Multiset.Entry<String> entry: words.entrySet()) {
				String word = entry.getElement();
				int count = entry.getCount();
				Integer wordId = dictionary.get(word);
				Long freq = docFreq.get(wordId);
				double tfIdfValue = tfidf.calculate(count, freq.intValue(), wordCount, docCount);
				vector.setQuick(wordId, tfIdfValue);
			}
			
			// With the classifier, we get one score for each label 
			Vector resultVector = classifier.classifyFull(vector);
			HashMap<Integer, Double> topScores;
			topScores.put(-1, 0.0);
			topScores.put(-2, 0.0);
			topScores.put(-3, 0.0);
			int bestCategoryId = -1;
			double minimum;
			
			for(Element element: resultVector.all()) {
				
				int categoryId = element.index();
				double score = element.get();
				
				if ((minimum = Collections.min(topScores.values())) < score){
					topScores.put(categoryId, score);
					topScores.values().remove(minimum);
				}
				System.out.print("  " + labels.get(categoryId));
			}
			System.out.println(" => " + labels.get(bestCategoryId));
		}
		analyzer.close();
		reader.close();
	}
}
