package ru.mvz.elasticsearch.repository;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.charset.Charset;
import java.nio.file.Path;
import java.text.DateFormat;

public interface IndexDefinition {

    Path getPath(String fileName);

    Charset getCharset();
}
