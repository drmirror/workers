package net.drmirror.workers;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoWriteException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.ReturnDocument;

/**
 * A Worker is an actor that processes every document in a collection, one by one.
 * Many Workers can be launched, each will automatically pick a part of the collection
 * and process those documents. Workers may die prematurely, in this case another Worker
 * will clean up the partially completed work and process that part of the collection again.
 *
 * @author andre.spiegel@mongodb.com 
 */
public abstract class Worker {
	
    private static long BACKOFF_MILLIS = 100;
    private static long MAX_LOCK_MILLIS = 1000;
    private static long HEARTBEAT_MILLIS = 10000;
    private static long MAX_MISSED_HEARTBEATS = 2;
    
    private String _id = java.util.UUID.randomUUID().toString();
    
	protected MongoClient client;
	protected MongoDatabase db;
	protected String dataCollectionName;
	protected MongoCollection<Document> dataCollection;
	protected MongoCollection<Document> workCollection;
	protected String fieldName;
	
	private int numUnits = -1;
	
	private boolean cleanup = false;
	private boolean workTableLockedByUs = false;

	private int numUnit = -1;
	private Object lowerBound = null;
	private Object upperBound = null;
	
	private Thread myThread = null;
	
	public Worker (MongoClient client,
			       String dbName,
			       String collectionName,
			       String fieldName,
			       int numUnits) {
		this.client = client;
		this.db = client.getDatabase(dbName);
		this.dataCollectionName = collectionName;
		this.dataCollection = db.getCollection(collectionName, Document.class);
		this.workCollection = db.getCollection("work", Document.class);
		this.fieldName = fieldName;
		this.numUnits = numUnits;
		ensureWorkTable();
		initialize();
	}
	
	public String _id() {
	    return this._id;
	}
	
	/**
	 * Make sure there is is a "work" collection in the database and that
	 * it has a document that serves as our work table.
	 */
	private void ensureWorkTable() {
		workCollection.createIndex(new Document("collection", 1).append("field", 1),
				                   new IndexOptions().unique(true));
		try {
			workCollection.insertOne(new Document("collection", dataCollectionName)
			                              .append("field", fieldName)
		                                  .append("lock", false)
		                                  .append("ts", new Date()));
		} catch (MongoWriteException ex) {
			// ignore duplicate key, this simply means we already have a work table
			if (ex.getCode() != 11000) throw ex;
		}
	}
	
	private void initialize() {
		Document workTable = null;
		try {
			workTable = acquireWorkTable();
			@SuppressWarnings("unchecked")
			List<Document> units = (List<Document>)workTable.get("units");
			if (units == null || allUnitsCompleted(units)) { 
			    initializeUnits(workTable);
			}
			if (pickUnit(workTable)) {
				launchProcessing();
			}
		} finally {
			releaseWorkTable(workTable);
		}
	}
	
	private boolean allUnitsCompleted(List<Document> units) {
	    for (Document unit : units) {
	        if (!"completed".equals(unit.getString("status")))
	            return false;
	    }
	    return true;
	}
	
	private Document acquireWorkTable() {
		Document result = null;
		while (true) {
		  result = workCollection.findOneAndUpdate(
		    new Document("collection", dataCollectionName)
		       .append("field",fieldName)
		       .append("lock", false),
		    new Document("$set", new Document("lock", true).append("ts", new Date())),
		    new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));
		  if (result != null) {
			  workTableLockedByUs = true;
			  break;
		  }
		  try {
		      checkStuckLock();
		      double jitter = 0.9 + 0.2 * Math.random();
		      int interval = (int)(jitter * (double)BACKOFF_MILLIS);
		      Thread.sleep(interval);
		  } catch (InterruptedException ex) { 
			  throw new RuntimeException(ex);
		  }
		}
		return result;
	}

	private void releaseWorkTable(Document workTable) {
		if (!workTableLockedByUs) return;
		workTable.put("lock", false);
		workTable.put("ts", new Date());
		workCollection.replaceOne(new Document("_id", workTable.get("_id")), workTable);
		workTableLockedByUs = false;
	}
	
	private void checkStuckLock() {
	    Document query = new Document("collection", dataCollectionName)
	                          .append("field", fieldName);
	    Document workTable = workCollection.find(query).first();

	    if (workTable.getBoolean("lock") == false) return;
	    // if we don't have a unit list yet, don't consider the lock stuck,
	    // because computing the units might take a long time
	    if (workTable.get("units") == null) return;
	    Date ts = workTable.getDate("ts");
	    long age = System.currentTimeMillis() - ts.getTime();
	    if (age > MAX_LOCK_MILLIS) {
	        // Lock is stuck, clear it. Because we also search
	        // for the time stamp we saw earlier, it is guaranteed
	        // that at most one process will succeed clearing the lock
	        System.out.printf("worker %s finding lock stuck for %d millis, clearing it\n", _id, age);
	        workCollection.findOneAndUpdate(
	            new Document("collection", dataCollectionName)
	                 .append("field", fieldName)
	                 .append("ts", ts),
	            new Document("$set", new Document("lock", false)
	                                      .append("ts", new Date())));
	    }
	}
	
	private void initializeUnits(Document workTable) {
		SplitFinder sf = new SplitFinder(dataCollection, fieldName, numUnits);
		List<Document> units = new ArrayList<Document>();
		for (SplitFinder.Range r : sf.getRanges()) {
			Document unit = new Document("lower_bound", r.lowerBound)
			                     .append("upper_bound", r.upperBound)
			                     .append("status", "open")
			                     .append("ts", new Date());
			units.add(unit);
		}
		workTable.put("units", units);
	}
	
	private boolean pickUnit(Document workTable) {
		@SuppressWarnings("unchecked")
		List<Document> units = (List<Document>)workTable.get("units");
		// if there's a stale unit, mark it for cleanup
		for (int i=0; i<units.size(); i++) {
		    Document unit = units.get(i);
		    if ("processing".equals(unit.getString("status"))) {
		        Date ts = unit.getDate("ts");
		        if (System.currentTimeMillis() - ts.getTime() 
		                > MAX_MISSED_HEARTBEATS * HEARTBEAT_MILLIS) {
		            this.numUnit = i;
		            this.lowerBound = unit.get("lower_bound");
		            this.upperBound = unit.get("upper_bound");
		            this.cleanup = true;
		            unit.put("status", "cleanup");
		            unit.put("owner", _id);
		            unit.put("ts", new Date());
		            return true;
		        }
		    }
		}
		// find a regular open unit
		for (int i=0; i<units.size(); i++) {
			Document unit = units.get(i);
			if ("open".equals(unit.getString("status"))) {
				this.numUnit = i;
				this.lowerBound = unit.get("lower_bound");
				this.upperBound = unit.get("upper_bound");
				this.cleanup = false;
				unit.put("status", "processing");
				unit.put("owner", _id);
				unit.put("ts", new Date());
				return true;
			}
		}
		return false; // couldn't find any work
	}
	
	private void launchProcessing() {
		Runnable r = new Runnable() {
			public void run() { work(); }
		};
		myThread = new Thread(r);
		myThread.setDaemon(false);
		myThread.start();
	}
	
	private static class WorkerFiredException extends Exception {};
	
	private void work() {
	    long lastHeartbeat = System.currentTimeMillis();
	    startProcessing();
		while (true) {
		    if (!cleanup) {
		        startUnit(lowerBound, upperBound);
		        for (Document d : findUnit()) {
		            process(d);
		            if (System.currentTimeMillis() - lastHeartbeat > HEARTBEAT_MILLIS) {
		                try {
		                    writeHeartbeat();
		                } catch (WorkerFiredException ex) {
		                    fired(lowerBound, upperBound);
		                    return;
		                }
		                lastHeartbeat = System.currentTimeMillis();
		            }
		        }
		        finishUnit(lowerBound, upperBound);
		    } else {
		        cleanup(lowerBound, upperBound);
		    }
			Document workTable = acquireWorkTable();
			markUnitComplete(workTable);
			boolean haveWork = pickUnit(workTable);
			releaseWorkTable(workTable);
			if (!haveWork) break;
		}
		finishProcessing();
	}
	
	private void writeHeartbeat() throws WorkerFiredException {
	    Document workTable = acquireWorkTable();
	    List<Document> units = (List<Document>)workTable.get("units");
	    Document unit = units.get(numUnit);
	    String owner = unit.getString("owner");
	    if (owner == null || !owner.equals(_id)) {
	        releaseWorkTable(workTable);
	        throw new WorkerFiredException();
	    } else {
	        units.get(numUnit).put("ts", new Date());
	        releaseWorkTable(workTable);
	    }
	}
	
	private void markUnitComplete(Document workTable) {
		@SuppressWarnings("unchecked")
		List<Document> units = (List<Document>)workTable.get("units");
		Document unit = units.get(numUnit);
		if (!cleanup) {
		    unit.put("status", "completed");
		    unit.remove("owner");
		    unit.put("ts", new Date());
		} else {
		    unit.put("status", "open");
		    unit.remove("owner");
		    unit.put("ts", new Date());
		}
	}

	/**
	 * Called before this Worker starts processing its first unit.
	 */
	public void startProcessing() {}
	
	/**
	 * Called before this Worker processes the first document of a new unit.
	 */
	public void startUnit(Object lowerBound, Object upperBound) {}
	
	/**
	 * Main processing method. Override this to define what should happen to every document.
	 */
	public abstract void process (Document d);
	
	/**
	 * Called when this Worker realizes that somebody else took over its unit,
	 * because this Worker missed too many heart beats. This method gives it the chance
	 * to do some final cleanup, although it shouldn't interfere with the cleanup that
	 * the new Worker is already doing. The Worker will terminate after this method returns.
	 */
	public void fired (Object lowerBound, Object upperBound) {}

	/**
	 * Called after the worker has processed the last document of its current unit,
	 * but before it marks that unit as complete.
	 */
	public void finishUnit(Object lowerBound, Object upperBound) {}

	/**
	 * Called when a unit has been detected as stale. In this method, the worker should
	 * do whatever is necessary to turn the unit into the open state again, so that
	 * another worker can pick it up and process it as if nothing happened.
	 */
	public void cleanup(Object lowerBound, Object upperBound) {}
	
	/**
	 * Called when the worker finds no more open units to process.
	 */
	public void finishProcessing() {}
	
	private FindIterable<Document> findUnit() {
		Document rangePredicate = new Document();
		if (lowerBound != null) {
			rangePredicate.append("$gte", lowerBound);
		}
		if (upperBound != null) {
			rangePredicate.append("$lt", upperBound);
		}
		Document query = new Document(fieldName, rangePredicate);
		return dataCollection.find(query).sort(new Document(fieldName,1));
	}
	
}
