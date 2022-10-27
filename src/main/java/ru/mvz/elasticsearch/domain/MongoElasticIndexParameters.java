package ru.mvz.elasticsearch.domain;

import ru.mvz.elasticsearch.util.DocumentHelper;
import ru.mvz.elasticsearch.util.DocumentTree;

import java.util.List;
import java.util.Set;

public interface MongoElasticIndexParameters {

    String getIndex();

    String getTitle();

    String getType();

    String getCollection();

    Set<List<String>> getFields();

    String getSummaryFieldName();

    String getSummaryFieldSeparator();

    Set<List<String>> getSummaryFields();

    List<MongoElasticIndex.JoinedCollection> getJoinedCollections();

    DocumentTree geDocumentTree();

    DocumentHelper getDocumentHelper();
}
