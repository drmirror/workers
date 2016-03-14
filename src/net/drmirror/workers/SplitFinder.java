package net.drmirror.workers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.bson.Document;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/**
 * Divides a MongoDB collection into a set of equal-sized partitions, where each is 
 * defined as a range of values of a certain split field.  The application can read
 * each of these values in parallel via separate cursors obtained from this object.
 * 
 * Usage: SplitFinder s = new SplitFinder(db, collection, splitField, numSplits);
 *        // this creates the SplitFinder and causes it to determine the split boundaries
 *
 *        for (Document d : s.findRange(0)) { ... }
 *        // loop over the documents from the first split,
 *        // typically done in some kind of worker thread
 *
 * @author Andre Spiegel <andre.spiegel@mongodb.com>
 */
public class SplitFinder {

    protected MongoDatabase db;
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
	
	public SplitFinder (MongoDatabase db,
	                    MongoCollection<Document> coll,
	                    String splitField, int numRanges) {
	  this.db = db;
	  this.coll = coll;
	  this.splitField = splitField;
	  this.numRanges = numRanges;
	  ranges = new ArrayList<Range>();
	  computeSplits();
	}
	
	public SplitFinder (MongoDatabase db, MongoCollection<Document> coll, int numRanges) {
		this(db, coll, "_id", numRanges);
	}

	protected void computeSplits() {
	   if (numRanges == 1) {
	       ranges.add(new Range(null, null));
	       return;
	   }
	   Document collStats = new Document("collStats",
	                                     coll.getNamespace().getCollectionName().toString());
	   Document stats = db.runCommand(collStats);
	   int documentCount = stats.getInteger("count");
	   int avgSize = stats.getInteger("avgObjSize");
	   long chunkSize = (long)(2.0 * (double)documentCount * (double)avgSize / (double)numRanges);
	   
	   Document splitVectorCmd = new Document("splitVector", coll.getNamespace().toString())
	                                  .append("keyPattern", new Document(splitField, 1))
	                                  .append("maxChunkSizeBytes", chunkSize);
	   Document splitVector = db.runCommand(splitVectorCmd);
	   @SuppressWarnings("unchecked")
	   List<Document> splitKeys = (List<Document>)splitVector.get("splitKeys");
	   ranges.add(new Range(null, splitKeys.get(0).get(splitField)));
	   for (int i=1; i<splitKeys.size(); i++) {
	       ranges.add(new Range(splitKeys.get(i-1).get(splitField),
	                            splitKeys.get(i).get(splitField)));
	   }
	   ranges.add(new Range(splitKeys.get(splitKeys.size()-1).get(splitField),
	                        null));
	   numRanges = ranges.size();
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
		MongoDatabase db = c.getDatabase("test");
		MongoCollection<Document> coll = db.getCollection("data");
		
		SplitFinder s = new SplitFinder(db, coll, 1);
		for (int i=0; i<s.getNumRanges(); i++) {
		    System.out.println(i + " - " + s.getLowerBound(i) + " - " + s.getUpperBound(i));
		}
		
	}
	
}
