package ru.mvz.elasticsearch.util;
import java.util.*;

public interface ContextForEachTree {

    @FunctionalInterface
    public interface ValueConverter <T> {
        T convert(String value);
    }

    DocumentHelper getDocumentHelper();

    DocumentTree getDocumentTree();

    Map.Entry<String, Object> getCurrentItem();

    NodeType getTypeValue();

    String getKey();

    Object getValue();

    <T> T getValue(ValueConverter<T> converter) throws ConvertDataException;

    Stack<Map.Entry<String, Object>> getPathToCurrentItem();

    ArrayList<String> getCurrentPath();

    public ArrayList<String> getCurrentPath(String key);

    ArrayList<String> getCurrentPath(Map.Entry<String, Object> node);

}
