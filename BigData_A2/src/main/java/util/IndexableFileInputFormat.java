package util;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import edu.umd.cloud9.collection.Indexable;

/**
 * Abstract class representing a <code>FileInputFormat</code> for
 * <code>Indexable</code> objects.
 */
public abstract class IndexableFileInputFormat<K, V extends Indexable> extends
		FileInputFormat<K, V> {

}
