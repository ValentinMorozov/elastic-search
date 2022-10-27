package ru.mvz.elasticsearch.util;

import java.util.Iterator;
import java.util.Map;

public interface DocumentTree {

    Iterator<Map.Entry<String,Object>> getIterator(Object value);

    NodeType getNodeType(Object value);

    default void removeItem(Object object, Object key) {}

     int size(Object value);
}
