package net.drmirror.workers;

import java.util.Date;

import org.bson.Document;

import com.mongodb.MongoClient;

public class DemoWorker extends Worker {

    public DemoWorker(MongoClient client, String dbName, String collectionName,
            String fieldName, int numUnits) {
        super(client, dbName, collectionName, fieldName, numUnits);
    }

    public void startUnit(Object lowerBound, Object upperBound) {
        System.out.printf("%s: worker %s starting unit %s .. %s\n", new Date(), _id(), lowerBound, upperBound);
    }
    
    public void process(Document d) {
        try { Thread.sleep(500); } catch (InterruptedException e) {}
    }
    
    public void finishProcessing() {
        System.out.printf("%s: worker %s finished\n", new Date(), _id());
    }
    
    public void cleanup(Object lowerBound, Object upperBound) {
        System.out.printf("%s: worker %s cleanup unit %s .. %s\n", new Date(), _id(), lowerBound, upperBound);
        try { Thread.sleep(30000); } catch (InterruptedException e) {}
        System.out.printf("%s: worker %s cleanup unit %s .. %s finished\n", new Date(), _id(), lowerBound, upperBound);
    }
    
    public static void main(String[] args) {
        MongoClient client = new MongoClient("localhost:27017");
        new DemoWorker(client, "test", "data", "_id", 10);
    }


}
