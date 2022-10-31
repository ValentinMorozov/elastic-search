package ru.mvz.elasticsearch.controller;


import com.fasterxml.jackson.core.JsonProcessingException;
import org.bson.Document;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;
import ru.mvz.elasticsearch.service.IndexEvent;
import ru.mvz.elasticsearch.service.Indexer;
import ru.mvz.elasticsearch.service.NotFoundIndexDefinitionException;
import ru.mvz.elasticsearch.util.ConvertDataException;
import ru.mvz.elasticsearch.util.IllegalObjectIdException;

import java.io.IOException;
import java.nio.file.NoSuchFileException;

@RestController
@RequestMapping("/api")
class ApiController {

    final private Indexer indexer;

    public ApiController( Indexer indexer) {
        this.indexer = indexer;
    }

    @PostMapping(value = { "/index/refresh/{name}", "/index/refresh/{name}/{type}" })
    public Mono<Document> refresh(@PathVariable String name, @PathVariable(required = false) String type)
            throws IOException, ConvertDataException, NotFoundIndexDefinitionException {
        return indexer.refresh(name, type);
    }

    @PutMapping(value = { "/index/{id}/{name}", "/index/{id}/{name}/{type}" })
    public Mono<Document> index(@PathVariable String id, @PathVariable String name, @PathVariable(required = false) String type)
            throws IOException, ConvertDataException, NotFoundIndexDefinitionException, IllegalObjectIdException {
        return indexer.sendIndexEvent(new IndexEvent("index", id, name, type));
    }

    @DeleteMapping(value = { "/index/{id}/{name}", "/index/{id}/{name}/{type}" })
    public Mono<Document> delete(@PathVariable String id, @PathVariable String name, @PathVariable(required = false) String type)
            throws IOException, ConvertDataException, NotFoundIndexDefinitionException, IllegalObjectIdException {
        return indexer.sendIndexEvent(new IndexEvent("delete", id, name, type));
    }


    @ExceptionHandler(IllegalObjectIdException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Mono<Document> handleIllegalObjectIdException(IllegalObjectIdException e) {
        return Mono.just(new Document("Error", e.getCause().getMessage()));
    }
    @ExceptionHandler(NotFoundIndexDefinitionException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Mono<Document> handleNotFoundIndexDefinitionException(NotFoundIndexDefinitionException e) {
        return Mono.just(new Document("Error", e.getMessage()));
    }

    @ExceptionHandler(JsonProcessingException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Mono<Document> handleJsonProcessingException(JsonProcessingException e) {
        return Mono.just(new Document("Error", e.getMessage()));
    }
    @ExceptionHandler(IOException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Mono<Document> handleIOException(IOException e) {
        return Mono.just(new Document("Error", e.getMessage()));
    }

    @ExceptionHandler(NoSuchFileException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Mono<Document> handleNoSuchFileException(NoSuchFileException e) {
        return Mono.just(new Document("Error, NoSuchFile ", e.getMessage()));
    }
}
