package ru.mvz.elasticsearch.repository;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.Random;
import java.util.stream.Stream;

@Component
public class FileStorage {

    final private Path path;

    public FileStorage(@Qualifier("fileStorage") Path path) {
        this.path = path;
    }

    public void writeCollection2Files(Collection<Object> collection) {
        String ext = ".dat";
        Random random = new Random();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-ddHH mm-ss-SSS_");
        collection.forEach(data -> {
            String fileName = String.format("%s%06d",simpleDateFormat.format(new Date()), random.nextInt(100000))+ext;
                    try {
                        Files.write(path.resolve(fileName), ((String)data).getBytes());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
        );
    }

    public Flux<String> pullFileContents() {
        Stream<Path> directoryStream;
        try {
            directoryStream = Files.list(path);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return Flux.fromStream(directoryStream).map(file -> {
            try {
                String data = new String(Files.readAllBytes(file));
                Files.delete(file);
                return data;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }
}
