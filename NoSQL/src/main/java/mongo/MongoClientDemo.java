package mongo;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.function.Consumer;

public class MongoClientDemo {

    public static void main(String[] args) {

        MongoClient mongo = new MongoClient("localhost", 27017);
        MongoDatabase database = mongo.getDatabase("uni_db");
        MongoCollection<Document> profCollection = database.getCollection("professoren");

        BasicDBObject searchQuery = new BasicDBObject();
        //searchQuery.put("Name", "Sokrates");
        FindIterable<Document> results = profCollection.find(searchQuery);
        results.forEach((Consumer) System.out::println);

    }

}
