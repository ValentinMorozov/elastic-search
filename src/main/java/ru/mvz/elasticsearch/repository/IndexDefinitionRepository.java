package ru.mvz.elasticsearch.repository;

import java.io.IOException;
import java.util.Optional;
import org.bson.Document;

public interface IndexDefinitionRepository {

    Document findByNameType(String name, String type) throws IOException;

    boolean save(String name, String type, Document document);

}
