package ru.mvz.elasticsearch.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;

import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.nonNull;

@Getter
public class DocumentHelper {
    DocumentTree documentTree;
    ObjectMapper objectMapper;
    @FunctionalInterface
    public interface ValueReceiver {
        ValueReceiver receive(ContextForEachTree ctx) throws ConvertDataException, JsonProcessingException;
    }

    public DocumentHelper(DocumentTree documentTree, ObjectMapper objectMapper) {
        this.documentTree = documentTree;
        this.objectMapper = objectMapper;
    }
    /**
     * Запускает процесс обработки дерева Document. Для каждого элемента вызывается метод receive интерфейса ValueReceiver.
     * Метод receive возвращает null или новый объект, реализующий интерфейс ValueReceiver, в котором реализована
     * приёмка атрибута. null означает, что дальнейшая обработка атрибута не требуется.
     * @param receiver объект, принимающий элементы дерева документа
     */

    final public void forEachTree(ValueReceiver receiver, Object rootNode)
            throws ConvertDataException, JsonProcessingException {
        if(nonNull(rootNode)) {
            ContextForEachTreeImpl context = new ContextForEachTreeImpl(this);
            context.setKeyValue("", rootNode);
            processingValue(receiver, context);
        }
    }
    /**
     * Перебирает элементы дерева Document. Для каждого элемента вызывается метод receive интерфейса ValueReceiver.
     * Метод receive возвращает null или новый объект, реализующий интерфейс ValueReceiver, в котором реализована
     * приёмка атрибута. null означает, что дальнейшая обработка атрибута не требуется.
     * @param receiver объект, принимающий элементы дерева документа
     * @param context набор параметров
     */
    private void processingValue(ValueReceiver receiver, ContextForEachTreeImpl context) throws ConvertDataException, JsonProcessingException {
        if(nonNull(receiver)) {
            ValueReceiver valueReceiver = receiver.receive(context);
            Map.Entry<String, Object> entry = context.getCurrentItem();
            Iterator<Map.Entry<String, Object>> collection = documentTree.getIterator(entry.getValue());
            if(collection.hasNext()) {
                context.push(entry);
                while(collection.hasNext()) {
                    Map.Entry<String, Object> node = collection.next();
                    processingValue(valueReceiver, context.setKeyValue(node.getKey(), node.getValue()));
                }
                context.pop();
            }
        }
    }

    final public void scanRemoveTree(BiFunction<ContextForEachTree, Map.Entry<String, Object>, TestRemoveResult> testRemove, Object rootNode) {
        if(nonNull(rootNode)) {
            ContextForEachTreeImpl context;
            context = new ContextForEachTreeImpl(this);
            context.setKeyValue("", rootNode);
            processingRemoveNode(testRemove, context);
        }
    }

    private void processingRemoveNode(BiFunction<ContextForEachTree, Map.Entry<String, Object>, TestRemoveResult> testRemove,
                                            ContextForEachTreeImpl context) {
        if(nonNull(testRemove)) {
            Map.Entry<String, Object> entry = context.getCurrentItem();
            if(documentTree.getNodeType(entry.getValue()) != NodeType.VALUE) {
                Iterator<Map.Entry<String, Object>> collection = documentTree.getIterator(entry.getValue());
                if (collection.hasNext()) {
                    context.push(entry);
                    ArrayList<String> keysForRemove = new ArrayList<>();
                    while (collection.hasNext()) {
                        Map.Entry<String, Object> node = collection.next();
                        TestRemoveResult testResult = testRemove.apply(context, node);
                        if(testResult == TestRemoveResult.REMOVE) {
                            keysForRemove.add(node.getKey());
                        }
                        else if(testResult == TestRemoveResult.NO_REMOVE) {
                            continue;
                        }
                        else if(documentTree.getNodeType(node.getValue()) != NodeType.VALUE) {
                            processingRemoveNode(testRemove, context.setKeyValue(node.getKey(), node.getValue()));
                            if(documentTree.size(entry.getValue()) == 0)
                                keysForRemove.add(node.getKey());
                        }
                    }
                    keysForRemove
                        .forEach(key -> documentTree.removeItem(entry.getValue(), key));
                    context.pop();
                }
            }
        }
    }

    final public List<List<String>> toList(Object rootNode) throws ConvertDataException, JsonProcessingException {
        List<List<String>> pathToValue = new ArrayList<>();
        DocumentHelper.ValueReceiver receiver = new DocumentHelper.ValueReceiver() {
            @Override
            public DocumentHelper.ValueReceiver receive(ContextForEachTree ctx) throws ConvertDataException {
                if(getDocumentTree().getNodeType(ctx.getValue()) == NodeType.VALUE) {
                    pathToValue.add(
                            Stream.concat(
                                Stream.concat(
                                        ctx.getPathToCurrentItem().stream().map(Map.Entry::getKey),
                                        Stream.of(ctx.getCurrentItem().getKey()))
                                    .filter(p ->  !(p.isEmpty() || Character.isDigit(p.charAt(0)))),
                                    Stream.of((String)ctx.getValue(String::valueOf)))
                                .collect(Collectors.toCollection(ArrayList::new)));
                }
                return this;
            }
        };
        forEachTree(receiver, rootNode);
        return pathToValue;
    }

}
