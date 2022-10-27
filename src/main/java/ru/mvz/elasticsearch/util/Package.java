package ru.mvz.elasticsearch.util;

import org.bson.Document;
import org.bson.types.ObjectId;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

public class Package {
    private static SimpleDateFormat sdf =
            new
                    SimpleDateFormat(
                    "yyyy-MM-dd HH:mm:ss.SSS"
            );

    public static String formatDate(Date date) {
        return sdf.format(date);
    }

    public static String getStringWithDefault(String string, String defaultValue) {
        if(isNull(string) || string.isEmpty()) {
            return defaultValue;
        }
        return string;
    }

    public static int compareArrayList(List<String> o1, List<String> o2) {
        int result = 0;
        for(int i = 0, sizeO2 = o2.size(), sizeO1 = o1.size(); result == 0; i++) {
            if(i < sizeO1 && i < sizeO2)
                result = o1.get(i).compareTo(o2.get(i));
            else {
                if (i >= sizeO1)
                    result = -1;
                if (i >= sizeO2)
                    result += 1;
                break;
            }
        }
        return result;
    }

    @SafeVarargs
    public static List<List<String>> joinArrayList(Set<? extends List<String>>... lists) {
        List<List<String>> filteredList = new ArrayList<>();

        Stream.of(lists)
                .filter(Objects::nonNull)
                .flatMap(Collection::stream)
/*                .map(e -> e.stream()
                        .flatMap(line -> Arrays.stream(line.split("\\.")))
                        .collect(Collectors.toCollection(ArrayList::new)))*/
                .sorted(Package::compareArrayList)
                .forEachOrdered(it -> {
                    int size = filteredList.size();
                    if(size == 0 || notInclude(filteredList.get(size - 1), it))
                        filteredList.add(it);

                });
        return filteredList;
    }

    public static boolean notInclude(List<String> prev, List<String> current) {
        boolean result = false;
        int sizeCurrent = current.size(), i = 0;

        for(String it: prev) {
            if(i >= sizeCurrent || !it.equals(current.get(i++))) {
                result = true;
                break;
            }
        }
        return result;
    }

    public static List<List<String>> splitByDot(Stream<? extends List<String>> stream) {
        return stream.map(e -> e.stream()
                        .flatMap(line -> Arrays.stream(line.split("\\.")))
                        .collect(Collectors.toCollection(ArrayList::new)))
                .collect(Collectors.toList());
    }

    public static <T extends List<String>> List<T> beginsSublists(T list) {
        if(list.size() < 2)
            return new ArrayList<>();
        List<T> sublists = beginsSublists((T)list.subList(0, list.size()-1));
        sublists.add((T)list.subList(0, list.size()-1));
        return sublists;
    }

    static public Document idDocument(String id) throws IllegalObjectIdException {
        try {
            return new Document("_id",
                    id.isEmpty()
                            ? new ObjectId()
                            : new ObjectId(id));
        }
        catch(Exception e) {
            throw new IllegalObjectIdException("Error", e);
        }
    }
    static public List<String> throwable2ListMessage(Throwable ex) {
        List msgList = new ArrayList();
        for(Throwable e = ex; nonNull(e); e = e.getCause()) {
            msgList.add(e.getMessage());
        }
        return msgList;
    }
}
