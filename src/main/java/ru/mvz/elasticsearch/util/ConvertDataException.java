package ru.mvz.elasticsearch.util;

public class ConvertDataException extends Exception {
    public ConvertDataException(String message) {
        super(message);
    }
    public ConvertDataException(String message, Throwable cause) {
        super(message, cause);
    }
}
