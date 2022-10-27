package ru.mvz.elasticsearch.repository;

import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import lombok.Getter;
import org.bson.Document;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import ru.mvz.elasticsearch.util.IllegalObjectIdException;
import ru.mvz.elasticsearch.util.Package;

@Repository
@Getter
public class ReactorRepositoryMongoDB {
    private MongoDatabase mongoDatabase;

    public MongoCollection<Document> getCollection(String collectionName) {
        return getMongoDatabase().getCollection(collectionName);
    }

    public ReactorRepositoryMongoDB(MongoDatabase mongoDatabase) {
        this.mongoDatabase = mongoDatabase;
    }

    public Flux<Document> findById(String collectionName, String id, Document projections)  throws IllegalObjectIdException {
        return Flux.from(getCollection(collectionName).find(Package.idDocument(id)).projection(projections));
    }
    public Flux<Document> find(String collectionName, Document expression, Document projections) {
        return Flux.from(getCollection(collectionName).find(expression).projection(projections));
    }

    public Flux<Document> findAll(String collectionName, Document projections) {
//        List<Document> coll = Flux.from(getCollection(collectionName).find().projection(projections)).toStream().collect(Collectors.toList());
        return Flux.from(getCollection(collectionName).find().projection(projections));
    }

}
