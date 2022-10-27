package ru.mvz.elasticsearch.util;

public class IllegalObjectIdException extends Exception  {
    public IllegalObjectIdException(String message) {
        super(message);
    }
    public IllegalObjectIdException(String message, Throwable cause) {
        super(message, cause);
    }
}
