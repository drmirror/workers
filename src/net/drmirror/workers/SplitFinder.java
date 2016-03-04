package net.drmirror.workers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/**
 * Divides a MongoDB collection into a set of equal-sized partitions, where each is 
 * defined as a range of values of a certain split field.  The application can read
 * each of these values in parallel via separate cursors obtained from this object.
 * 
 * Usage: SplitFinder s = new SplitFinder(collection, splitField, numSplits);
 *        // this creates the SplitFinder and causes it to determine the split boundaries
 *
 *        for (Document d : s.findRange(0)) { ... }
 *        // loop over the documents from the first split,
 *        // typically done in some kind of worker thread
 *
 * @author Andre Spiegel <andre.spiegel@mongodb.com>
 */
public class SplitFinder {

	protected MongoCollection<Document> coll;
	protected String splitField;
	protected int numRanges;
	
	public static class Range {
		Object lowerBound;
		Object upperBound;
		public Range (Object lowerBound, Object upperBound) {
			this.lowerBound = lowerBound;
			this.upperBound = upperBound;
		}
		public String toString() {
			return "[" + lowerBound + "," + upperBound + ")";
		}
	}
	
	protected List<Range> ranges;
	
	public SplitFinder (MongoCollection<Document> coll, String splitField, int numRanges) {
	  this.coll = coll;
	  this.splitField = splitField;
	  this.numRanges = numRanges;
	  ranges = new ArrayList<Range>();
	  computeSplits();
	}
	
	public SplitFinder (MongoCollection<Document> coll, int numRanges) {
		this(coll, "_id", numRanges);
	}

	/**
	 * Computes the ranges for the splits by reading all values of the split fields
	 * into an array and then dividing it into equal partitions.  Override this method
	 * to implement other strategies, for example more heuristic ones that avoid
	 * reading the whole collection.
	 */
	protected void computeSplits() {
		if (numRanges == 1) return;
		long count = coll.count();
		List<Object> values = new ArrayList<Object>((int)count);
		Document projection = splitField.equals("_id") 
				            ? new Document(splitField, 1)
		                    : new Document(splitField, 1).append("_id", 0);
		for (Document d : coll.find().projection(projection).sort(new Document(splitField, 1))) {
			values.add(d.get(splitField));
		}
		int splitStep = values.size() / numRanges;

		ranges.add(new Range(null, values.get(splitStep)));
		for (int i=1; i<numRanges-1; i++) {
		    ranges.add(new Range(values.get(i*splitStep), values.get((i+1)*splitStep)));
		}
		ranges.add(new Range(values.get(splitStep*(numRanges-1)), null));
		
		values.clear();
	}
	
	public int getNumRanges() {
		return numRanges;
	}
	
	public Object getLowerBound (int numRange) {
		return ranges.get(numRange).lowerBound;
	}
	
	public Object getUpperBound (int numRange) {
		return ranges.get(numRange).upperBound;
	}
	
	public List<Range> getRanges() {
		return Collections.unmodifiableList(ranges);
	}

	/**
	 * Returns an iterable cursor over the nth range of the collection
	 * @param numSplit number of the split, from the range 0..numSplits
	 */
	public FindIterable<Document> findRange (int numRange) {
		if (numRange < 0 || numRange >= numRanges) {
			String message = String.format(
			    "numSplit is %d, must be in range [0..%d]", numRange, numRanges
			);
			throw new RuntimeException(message);
		}
		if (numRanges == 1) {
			return coll.find();
		}
		Range r = ranges.get(numRange);
		if (numRange == 0) {
			return coll.find(new Document(splitField, new Document ("$lt", r.upperBound)));
		} else if (numRange == numRanges-1) {
			return coll.find(new Document(splitField, new Document ("$gte", r.lowerBound)));
		} else {
			return coll.find(new Document(splitField,
			    new Document ("$gte", r.lowerBound).append ("$lt", r.upperBound)
			));
		}
	}
	
	public static void main(String[] args) throws Exception {
		MongoClient c = new MongoClient("127.0.0.1:27017");
		MongoDatabase db = c.getDatabase("cliente360");
		MongoCollection<Document> coll = db.getCollection("cu_1_2015-12-22");
		
		SplitFinder s = new SplitFinder(coll, 16);
		
	}
	
}
