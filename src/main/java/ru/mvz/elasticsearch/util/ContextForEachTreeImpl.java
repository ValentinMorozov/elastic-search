package ru.mvz.elasticsearch.util;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

import java.util.*;
import java.util.stream.Collectors;

import static java.util.Objects.nonNull;

@Getter
public class ContextForEachTreeImpl implements ContextForEachTree {
    DocumentHelper documentHelper;

    /** Переменная для хранения ключа атрибута. */
    @Setter(AccessLevel.PACKAGE)
    private Map.Entry<String, Object> currentItem;
    /** Переменная для хранения текущего пути к атрибуту. */
    final private Stack<Map.Entry<String, Object>> pathToCurrentItem = new Stack<>();
    /** Переменная для хранения объекта обработки исключений **/

    ContextForEachTreeImpl(DocumentHelper documentHelper) {
        this.documentHelper = documentHelper;
    }

    public ContextForEachTreeImpl setKeyValue(String key, Object value) {
        this.currentItem = new AbstractMap.SimpleEntry<>(key, value);
        return this;
    }

    public Map.Entry<String, Object> push(Map.Entry<String, Object> node) {
        return pathToCurrentItem.push(node);
    }

    public Map.Entry<String, Object> pop() {
        return pathToCurrentItem.pop();
    }

    @Override
    public DocumentTree getDocumentTree() {
        return documentHelper.getDocumentTree();
    }

    public NodeType getTypeValue() {
        return getDocumentTree().getNodeType(getValue());
    }

    @Override
    public String getKey() {
        return getCurrentItem().getKey();
    }

    @Override
    public Object getValue() {
        return getCurrentItem().getValue();
    }

    @Override
    public ArrayList<String> getCurrentPath() {
        return getPathToCurrentItem().stream().map(Map.Entry::getKey)
                .filter(p ->  !(p.isEmpty() || Character.isDigit(p.charAt(0))))
                .collect(Collectors.toCollection(ArrayList::new));
    }

    @Override
    public ArrayList<String> getCurrentPath(Map.Entry<String, Object> node) {
        return getCurrentPath(node.getKey());
    }

    @Override
    public ArrayList<String> getCurrentPath(String key) {
        ArrayList<String> keyPath = getCurrentPath();
        if(!key.isEmpty() && !Character.isDigit(key.charAt(0)))
            keyPath.add(key);
        return keyPath;
    }

    public <T> T getValue(ValueConverter<T> converter) throws ConvertDataException {
        T result = null;
        if(getTypeValue() == NodeType.VALUE) {
            try {
                    result = nonNull(converter) ?
                            converter.convert((String)getValue()) :
                            (T)getValue();
            }
            catch (Exception e) {
                ConvertDataException exception = new ConvertDataException(buildMsgInfo("Convert:"), e);
//                if(!ctx.onError.test(exception))
                throw exception;
            }
        }
        else {
            ConvertDataException exception = new ConvertDataException(buildMsgInfo("Type is not an value node:"));
//            if(!ctx.onError.test(exception))
            throw exception;
        }
        return result;
    }

    public String buildMsgInfo(String textMsg) {
        return textMsg + " " +
                "\"" + getKey() + "\"" +
                " path " + getPathToCurrentItem().stream()
                .map(p -> p.getKey())
                .collect(Collectors.joining("\\", "", "\\")) +
                getKey() + ": " + getValue();
    }
}
