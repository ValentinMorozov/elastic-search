package ru.mvz.elasticsearch.util;

import lombok.Getter;
import org.bson.Document;

import java.util.*;
import java.util.stream.Collectors;
/**
 * Класс Context объединяет множество параметров в один объект, используется для минимизации количества передаваемы
 * параметров при вызове методов.
 *
 */
@Getter
public class DocumentBson implements DocumentTree {

    @Override
    final public NodeType getNodeType(Object value) {
        return value instanceof Document ? NodeType.NODE
                : value instanceof List ? NodeType.ARRAY
                : NodeType.VALUE;
    }
    @Override
    final public Iterator<Map.Entry<String,Object>> getIterator(Object value) {
        NodeType nodeType = getNodeType(value);
        int[] i = {0};
        return nodeType == NodeType.NODE ? ((Document) value).entrySet().iterator()
                : (nodeType == NodeType.ARRAY ? ((List<Object>) value) : Collections.emptyList())
                .stream()
                .collect(Collectors.toMap(val -> String.valueOf(i[0]++), val -> val))
                .entrySet().iterator();
    }
    @Override
    final public int size(Object value) {
        NodeType nodeType = getNodeType(value);
        return nodeType == NodeType.NODE ? ((Document) value).size()
                : (nodeType == NodeType.ARRAY ? ((List<Object>) value).size() : 0);
    }
    @Override
    final public void removeItem(Object object, Object key) {
        if(object instanceof Document) {
            ((Document)object).remove(key);
        }
        else if(object instanceof ArrayList){
            ((ArrayList)object).remove(Integer.parseInt((String)key));
        }
    }
}
