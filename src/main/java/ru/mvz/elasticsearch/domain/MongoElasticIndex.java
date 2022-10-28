package ru.mvz.elasticsearch.domain;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.sun.istack.internal.NotNull;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.bson.Document;
import org.bson.types.ObjectId;
import ru.mvz.elasticsearch.util.*;
import ru.mvz.elasticsearch.util.Package;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

@Getter
public class MongoElasticIndex extends MongoFields {

    final private DocumentHelper documentHelper;

    final private DocumentTree documentTree;

    final private String index;

    final private String type;

    final private String collection;

    final private List<MongoElasticIndex.JoinedCollection> joinedCollections = new ArrayList<>();

    final private Set<List<String>> collectFieldAndJoinedFields;

    final private Set<List<String>> allFields;

    final private Set<List<String>> allFieldsPath;

    public MongoElasticIndex(MongoElasticIndexParameters mongoElasticIndexParameters) {

        this.documentTree = mongoElasticIndexParameters.geDocumentTree();

        this.documentHelper = mongoElasticIndexParameters.getDocumentHelper();

        this.index = mongoElasticIndexParameters.getIndex();
        this.type = mongoElasticIndexParameters.getType();
        this.collection = mongoElasticIndexParameters.getCollection();

        this.setFields(mongoElasticIndexParameters.getFields());

        this.setSummaryFieldFields(mongoElasticIndexParameters.getSummaryFields());
        this.setSummaryFieldName(this.getSummaryFieldFields().size() > 0 ?
                Package.getStringWithDefault(mongoElasticIndexParameters.getSummaryFieldName(), "summaryField")
                : "");

        mongoElasticIndexParameters
                .getJoinedCollections()
                .forEach(p -> p.setProjection(Package.joinArrayList(p.getFields(), p.getSummaryFieldFields())));

        this.joinedCollections.addAll(mongoElasticIndexParameters.getJoinedCollections());
        this.joinedCollections.forEach(JoinedCollection::buildJoinedFields);

        this.setSummaryFieldSeparator(mongoElasticIndexParameters.getSummaryFieldSeparator());

        this.collectFieldAndJoinedFields = new HashSet<>(Package.joinArrayList(this.getSummaryFieldFields(),
                extractFromJoinedCollection(JoinedCollection::getLocalFields)));

        this.allFields = new HashSet<>(Package.joinArrayList(this.getFields(),
                extractFromJoinedCollection(JoinedCollection::getJoinedFields)));

        this.allFieldsPath = generateSublistsFields(getAllFields());

    }

    public String deleteBuild(Document document)
            throws ConvertDataException, JsonProcessingException{
        StringBuilder stringBuilder = new StringBuilder();
        actionBuild("delete", document, stringBuilder);
        return stringBuilder.toString();
    }

    public String indexBuild(Document document)
            throws ConvertDataException, JsonProcessingException {
       StringBuilder stringBuilder = new StringBuilder();
       actionBuild("index", document, stringBuilder);

       if(getSummaryFieldFields().size() > 0) {
           stringBuilder.append("{");
           appendKey(getSummaryFieldName(), stringBuilder);
           stringBuilder.append("\"");
           documentHelper.forEachTree(summaryReceiver(stringBuilder), document);
           stringBuilder.append("\"");
       }

       Map<String, StringBuilder> joinedSummaryMap = null;
       for(JoinedCollection joinedCollection: getJoinedCollections()) {
           if(joinedCollection.getSummaryFieldFields().size() > 0) {
               String summaryFieldName = Package.getStringWithDefault(joinedCollection.getSummaryFieldName(), getSummaryFieldName());
               StringBuilder currentStringBuilder = stringBuilder;
               if(!getSummaryFieldName().equals(summaryFieldName)) {
                   if(isNull(joinedSummaryMap)) {
                       joinedSummaryMap = new HashMap<>();
                   }
                   currentStringBuilder = joinedSummaryMap.get(summaryFieldName);
                   if(isNull(currentStringBuilder)) {
                       currentStringBuilder = new StringBuilder();
                       appendKey(summaryFieldName, currentStringBuilder);
                       currentStringBuilder.append("\"");
                       joinedSummaryMap.put(summaryFieldName, currentStringBuilder);
                   }
               }
               else if(currentStringBuilder.substring(stringBuilder.length()-1).equals("\"")) {
                   currentStringBuilder.setLength(stringBuilder.length()-1);
               }
               documentHelper.forEachTree(summaryReceiver(currentStringBuilder), document.get(joinedCollection.getJoinedFieldName()));
               currentStringBuilder.append("\"");
           }
       }

       if(nonNull(joinedSummaryMap)) {
           joinedSummaryMap.forEach((nameField, currentStringBuilder) -> {
               if(stringBuilder.substring(stringBuilder.length()-1).equals("\"")) {
                   stringBuilder.append(",");
                   stringBuilder.append(currentStringBuilder);
               }
           });
       }

       documentHelper.scanRemoveTree(testRemoveBuilder(getAllFields(), getAllFieldsPath()), document);

       int startSubstring = "\n".equals(stringBuilder.substring(stringBuilder.length()-1)) ? 0 : 1;

       String mainFields = documentHelper.getObjectMapper().writeValueAsString(document);
        if(mainFields.length() > 3) {
            if(stringBuilder.substring(stringBuilder.length()-1).equals("\"")) {
                stringBuilder.append(",");
            }
            stringBuilder.append(mainFields.substring(startSubstring));
        }
        else {
            stringBuilder.append("}");
        }

       stringBuilder.append("\n");
       return stringBuilder.toString();
    }

    private void actionBuild(String action, Document document, StringBuilder stringBuilder) {
        String id = document.get("_id").toString();
        stringBuilder.append("{" + "\"").append(action).append("\"").append(":{");
        appendKeyValue("_index", getIndex(), stringBuilder);
        String type = getType();
        if(nonNull(type) && !type.isEmpty()) {
            stringBuilder.append(',');
            appendKeyValue("_type", type, stringBuilder);
        }
        stringBuilder.append(',');
        appendKeyValue("_id", id, stringBuilder);
        stringBuilder.append("}}\n");
    }

    static private void appendKey(String key, StringBuilder stringBuilder) {
        stringBuilder.append("\"").append(key).append("\"").append(":");
    }

    static private void appendKeyValue(String key, String value, StringBuilder stringBuilder) {
        appendKey(key, stringBuilder);
        stringBuilder.append("\"").append(value).append("\"");
    }

    private Set<List<String>> extractFromJoinedCollection (Function<JoinedCollection,
            Collection<List<String>>> streamFunction) {
        return this.getJoinedCollections().stream()
                .map(p -> streamFunction.apply(p).stream()
                        .flatMap(Collection::stream)
                        .collect(Collectors.toCollection(ArrayList::new)))
                .collect(Collectors.toSet());
    }

    public List<JoinCondition> getJoinConditions(Document source) {
        final DocumentTree documentTree = getDocumentTree();
        return getJoinedCollections().stream()
                .map(p -> {
                    ArrayList<Object> values = p.getLocalFields().stream()
                            .map(s -> source.getEmbedded(s, Object.class))
                            .collect(Collectors.toCollection(ArrayList::new));

                    Document doc = new Document();
                    if(values.size() == p.getForeignFields().size()) {
                        p.getForeignFields()
                                .forEach(s -> {
                                    int i = doc.size();
                                    Object value = values.get(i);
                                    if(documentTree.getNodeType(value) == NodeType.VALUE) {
                                        if(p.getConvertLocalFields().size() > i) {
                                            String key = p.getConvertLocalFields().get(i);
                                            if("String".equals(key))
                                                value = value.toString();
                                            else if("ObjectId".equals(key))
                                                value = new ObjectId(value.toString());
                                        }
                                    }
                                    else {
/// ??? exception
                                    }
                                    doc.put(String.join(".", s), value);
                                });
                    }

                    return new JoinCondition(p, doc);
                })
                .collect(Collectors.toList());
    }

    private Set<List<String>> generateSublistsFields(Set<List<String>> fields) {
        return fields.stream()
                .flatMap(e -> Package.beginsSublists(e).stream())
                .collect(Collectors.toSet());
    }

    private DocumentHelper.ValueReceiver summaryReceiver(StringBuilder stringBuilder) {
        String collectFieldSeparator = getSummaryFieldSeparator();
        return new DocumentHelper.ValueReceiver() {
            public DocumentHelper.ValueReceiver receive(ContextForEachTree ctx) {
                ArrayList<String> currentPath = ctx.getCurrentPath(ctx.getKey());
                Object value = ctx.getValue();
                if (ctx.getTypeValue() == NodeType.VALUE &&
                     (value instanceof String || value instanceof Integer || value instanceof Long) &&
                     getSummaryFieldFields().contains(currentPath)) {
                    if(!"\"".equals(stringBuilder.substring(stringBuilder.length()-1))) {
                        stringBuilder.append(collectFieldSeparator);
                    }
                    stringBuilder.append(value);
                }
                return this;
            }
        };
    }

    private BiFunction<ContextForEachTree, Map.Entry<String, Object>, TestRemoveResult> testRemoveBuilder(
            Set<List<String>> notRemoved, Set<List<String>> tested) {
        return (ContextForEachTree context, Map.Entry<String, Object> node) -> {
            ArrayList<String> keyPath = context.getCurrentPath(node);
            return tested.contains(keyPath) ? TestRemoveResult.NEXT
                    : notRemoved.contains(keyPath)  ? TestRemoveResult.NO_REMOVE : TestRemoveResult.REMOVE;
        };
    }

    /**
     * Реализует интерфейс .
     *
     * @author  Валентин Морозов
     * @since   1.0
     */
    @Getter
    @Setter
    @NoArgsConstructor
    static public class JoinedCollection extends MongoFields {

        private String from;

        private List<List<String>> localFields;

        private List<List<String>> foreignFields;

        private List<String> convertLocalFields;

        private String joinedFieldName;

        private boolean summaryFieldOnly = false;

        public Set<List<String>> joinedFields;

        public void setLocalFields(@NotNull List<String> value) {
            this.localFields = Package.splitByDot(Stream.of(value));
        }
        public void setForeignFields(@NotNull List<String> value) {
            this.foreignFields =  Package.splitByDot(Stream.of(value));
        }

        public void setConvertLocalFields(@NotNull List<String> value) {
            this.convertLocalFields = new ArrayList<>(value);
        }

        public void buildJoinedFields() {
            setJoinedFields(getFields().stream()
                            .map(it -> Stream.concat(Stream.of(getJoinedFieldName()),
                                            it.stream())
                                    .collect(Collectors.toList()))
                    .collect(Collectors.toSet()));
        }
    }

    @Getter
    @Setter
    @AllArgsConstructor
    static public class JoinCondition {

        private JoinedCollection collection;

        private Document condition;
    }

}
