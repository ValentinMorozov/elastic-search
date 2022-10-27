package ru.mvz.elasticsearch.repository;

import lombok.Getter;
import lombok.NonNull;
import org.bson.Document;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.file.Files;

import static java.util.Objects.isNull;

@Service
@Getter
public class IndexDefinitionFileRepository implements IndexDefinitionRepository {

    final private IndexDefinition indexDefinition;
    IndexDefinitionFileRepository (IndexDefinition indexDefinition) {
        this.indexDefinition = indexDefinition;
    }

    @Override
    public Document findByNameType(@NonNull String indexName, String indexType) throws IOException {
        Document document = null;
        String buffer = new String(Files.readAllBytes(indexDefinition.getPath(fileNameBuild(indexName,indexType))),
                indexDefinition.getCharset());
        document = Document.parse(buffer);

/*
        try {
            String buffer = new String(Files.readAllBytes(indexDefinition.getPath(fileNameBuild(indexName,indexType))),
                    indexDefinition.getCharset());
            document = Document.parse(buffer);
        }
        catch (FileNotFoundException | NoSuchFileException e ) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }*/
        return document;
    }

    @Override
    public boolean save(String name, String type, Document document) {
        return false;
    }

    private String fileNameBuild(String indexName, String indexType) {
        return indexName
                + (isNull(indexType) || indexType.isEmpty()
                ? ""
                : "-" + indexType) + ".json";
    }
}
