package ru.mvz.elasticsearch.service;

public class HttpServiceException extends RuntimeException {
    final private int statusCode;
    public HttpServiceException(String message, int statusCode) {
        super(message);
        this.statusCode = statusCode;
    }
}