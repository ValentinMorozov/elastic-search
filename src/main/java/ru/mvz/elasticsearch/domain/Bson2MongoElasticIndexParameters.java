package ru.mvz.elasticsearch.domain;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.Getter;
import lombok.Setter;
import org.bson.Document;
import ru.mvz.elasticsearch.util.*;
import ru.mvz.elasticsearch.util.DocumentHelper.*;

import java.util.*;

import static java.util.Objects.nonNull;

@Setter
@Getter
public class Bson2MongoElasticIndexParameters implements MongoElasticIndexParameters {

    DocumentHelper documentHelper;

    private String index;

    private String title;

    private String type;

    private String collection;

    private Set<List<String>> fields;

    private List<MongoElasticIndex.JoinedCollection> joinedCollections = new ArrayList<>();

    private String summaryFieldName;

    private String summaryFieldSeparator = " ";

    private Set<List<String>> summaryFields;

    public Bson2MongoElasticIndexParameters(Document indexProperties, DocumentHelper documentHelper)
            throws ConvertDataException, JsonProcessingException {
        this.documentHelper = documentHelper;

        documentHelper.forEachTree(rootReceiver(documentHelper), indexProperties);

    }

    @Override
    public DocumentTree geDocumentTree() {
        return getDocumentHelper().getDocumentTree();
    }

    private ValueReceiver rootReceiver(DocumentHelper documentHelper) {
        return new ValueReceiver() {
            public ValueReceiver receive(ContextForEachTree ctx) throws ConvertDataException {
                ValueReceiver receiver = this;
                if (ctx.getTypeValue() == NodeType.NODE) {
                    switch (ctx.getKey()) {
                        case "source": receiver = sourceReceiver(documentHelper);
                            break;
                        default:
                            break;
                    }
                } else if (ctx.getTypeValue() == NodeType.VALUE) {
                    switch (ctx.getKey()) {
                        case "index": setIndex(ctx.getValue(String::valueOf));
                            break;
                        case "title": setTitle(ctx.getValue(String::valueOf));
                            break;
                        case "type": setType(ctx.getValue(String::valueOf));
                            break;
                        default:
                            break;
                    }
                }
                return receiver;
            }
        };
    }

    private ValueReceiver sourceReceiver(DocumentHelper documentHelper) {
        return (ContextForEachTree ctx) -> {
            ValueReceiver receiver = null;
            String key = ctx.getKey();
            if ((ctx.getTypeValue() == NodeType.ARRAY || ctx.getTypeValue() == NodeType.NODE)) {
                if ("fields".equals(key))
                    setFields(new HashSet<>(documentHelper.toList(ctx.getValue())));
                else if ("joinedCollections".equals(key)) {
                    joinedCollections = new ArrayList<>();
                    receiver = joinedCollectionReceiver(documentHelper);
                }
                else if ("summaryField".equals(key))
                    receiver = summaryFieldReceiver(documentHelper);

            } else if (ctx.getTypeValue() == NodeType.VALUE) {
                switch (ctx.getKey()) {
                    case "collection": setCollection(ctx.getValue(String::valueOf));
                        break;
                    default:
                        break;
                }
            }
            return receiver;
        };
    }

    private ValueReceiver summaryFieldReceiver(DocumentHelper documentHelper) {
        return (ContextForEachTree ctx) -> {
            String key = ctx.getKey();
            if (ctx.getTypeValue() == NodeType.ARRAY) { //joinedCollections
                if ("fields".equals(key))
                    setSummaryFields(new HashSet<>(documentHelper.toList(ctx.getValue())));
            } else if (ctx.getTypeValue() == NodeType.VALUE) {
                switch (ctx.getKey()) {
                    case "separator": setSummaryFieldSeparator(ctx.getValue(String::valueOf));
                        break;
                    case "as": setSummaryFieldName(ctx.getValue(String::valueOf));
                        break;
                    default:
                        break;
                }
            }
            return null;
        };
    }

    private ValueReceiver joinedCollectionReceiver(DocumentHelper documentHelper) {
        return (ContextForEachTree ctx) -> {
            ValueReceiver receiver = null;
            if ((ctx.getTypeValue() == NodeType.ARRAY || ctx.getTypeValue() == NodeType.NODE)) {
                joinedCollections.add(new MongoElasticIndex.JoinedCollection());
                receiver = joinedCollectionReceiverItem(documentHelper);
            }
            return receiver;
        };
    }

    private ValueReceiver joinedCollectionReceiverItem(DocumentHelper documentHelper) {
        return (ContextForEachTree ctx) -> {
            ValueReceiver receiver = null;
            String key = ctx.getKey();
            int last = joinedCollections.size() - 1;
            MongoElasticIndex.JoinedCollection currentItem = last >= 0 ? joinedCollections.get(last): null;

            if ((ctx.getTypeValue() == NodeType.ARRAY || ctx.getTypeValue() == NodeType.NODE)) { //joinedCollections
                if ("fields".equals(key)) {
                    if(last >= 0) {
                        joinedCollections.get(last).setFields(documentHelper.toList(ctx.getValue()));
                    }
                }
                else if ("summaryField".equals(key))
                    receiver = joinedCollectionSummaryFieldReceiver(documentHelper);
                else if ("localFields".equals(key) && ctx.getTypeValue() == NodeType.ARRAY && nonNull(currentItem))
                    currentItem.setLocalFields((ArrayList<String>)(ctx.getValue()));
                else if ("foreignFields".equals(key) && ctx.getTypeValue() == NodeType.ARRAY && nonNull(currentItem))
                    currentItem.setForeignFields((ArrayList<String>)(ctx.getValue()));
                else if ("convertLocalFields".equals(key) && ctx.getTypeValue() == NodeType.ARRAY && nonNull(currentItem))
                    currentItem.setConvertLocalFields((ArrayList)ctx.getValue());

            } else if (ctx.getTypeValue() == NodeType.VALUE) {
                if(last >= 0) {
                    switch (ctx.getKey()) {
                        case "from":
                            currentItem.setFrom(ctx.getValue(String::valueOf));
                            break;
                        case "localField":
                            currentItem.setLocalFields(Collections.singletonList(ctx.getValue(String::valueOf)));
                            break;
                        case "foreignField":
                            currentItem.setForeignFields(Collections.singletonList(ctx.getValue(String::valueOf)));
                            break;
                        case "as":
                            currentItem.setJoinedFieldName(ctx.getValue(String::valueOf));
                            break;
                        case "convertLocalField":
                            currentItem.setConvertLocalFields(Collections.singletonList(ctx.getValue(String::valueOf)));
                            break;
                        case "summaryFieldOnly":
                            currentItem.setSummaryFieldOnly(ctx.getValue(null));
                            break;
                        default:
                            break;
                    }
                }
            }
            return receiver;
        };
    }

    private ValueReceiver joinedCollectionSummaryFieldReceiver(DocumentHelper documentHelper) {
        return (ContextForEachTree ctx) -> {
            String key = ctx.getKey();
            int last = joinedCollections.size() - 1;
            if(last >= 0) {
                MongoElasticIndex.JoinedCollection currentItem = joinedCollections.get(last);
                if (ctx.getTypeValue() == NodeType.ARRAY) {
                    if ("fields".equals(key))
                        currentItem.setSummaryFieldFields(documentHelper.toList(ctx.getValue()));
                } else if (ctx.getTypeValue() == NodeType.VALUE) {
                    switch (ctx.getKey()) {
                        case "as":
                            currentItem.setSummaryFieldName(ctx.getValue(String::valueOf));
                            break;
                        default:
                            break;
                    }
                }
            }
            return null;
        };
    }
}
