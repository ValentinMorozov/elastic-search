package ru.mvz.elasticsearch.service;

public class NotFoundIndexDefinitionException extends Exception {
    public NotFoundIndexDefinitionException(String message) {
        super(message);
    }
}
