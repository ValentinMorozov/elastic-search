package ru.mvz.elasticsearch.service;

import org.bson.Document;
import org.springframework.stereotype.Component;
import ru.mvz.elasticsearch.domain.Bson2MongoElasticIndexParameters;
import ru.mvz.elasticsearch.domain.MongoElasticIndex;
import ru.mvz.elasticsearch.repository.IndexDefinitionRepository;
import ru.mvz.elasticsearch.util.ConvertDataException;
import ru.mvz.elasticsearch.util.DocumentHelper;

import java.io.IOException;
import java.util.*;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

@Component
public class MongoElasticIndexService {

    final private Map<List<String>, MongoElasticIndex> indexMap = Collections.synchronizedMap(new HashMap<>());

    final private IndexDefinitionRepository indexDefinitionRepository;

    final private DocumentHelper documentHelper;

    public MongoElasticIndexService(IndexDefinitionRepository indexDefinitionRepository, DocumentHelper documentHelper) {
        this.indexDefinitionRepository = indexDefinitionRepository;
        this.documentHelper = documentHelper;
    }

    public MongoElasticIndex get(String indexName, String indexType) throws IOException, ConvertDataException {
        MongoElasticIndex mongoElasticIndex = indexMap.get(Arrays.asList(indexName, indexType));
        if(isNull(mongoElasticIndex)) {
            Document indexDefinition = indexDefinitionRepository.findByNameType(indexName, indexType);
            if(nonNull(indexDefinition)) {
                mongoElasticIndex = new MongoElasticIndex(
                        new Bson2MongoElasticIndexParameters(indexDefinition, documentHelper));
            }
        }
        if(nonNull(mongoElasticIndex)) {
            put(mongoElasticIndex);
        }
        return mongoElasticIndex;
    }

    public void put(MongoElasticIndex mongoElasticIndex) {
        indexMap.put(Arrays.asList(
                mongoElasticIndex.getIndex(),
                mongoElasticIndex.getType()),
                mongoElasticIndex);
    }

    public MongoElasticIndex getWithException(String indexName, String indexType)
            throws IOException, ConvertDataException, NotFoundIndexDefinitionException {
        MongoElasticIndex mongoElasticIndex = get(indexName, indexType);
        if(isNull(mongoElasticIndex)) {
            throw new NotFoundIndexDefinitionException("Not found index definition: " + indexName + " " + indexType);
        }
        return mongoElasticIndex;
    }

}
