package ru.mvz.elasticsearch.config;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import ru.mvz.elasticsearch.repository.IndexDefinition;

import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;

@Getter
@Setter
@NoArgsConstructor
@ConfigurationProperties(prefix = "index.definition")
public class IndexDefinitionConfig {

    private String path;

    @Value("${:Cp1251}")
    private String charsetName;


    @Bean
    IndexDefinition indexDefinition() {
        return new IndexDefinition() {
            private Charset charset = Charset.forName(charsetName);

            @Override
            public Path getPath(String fileName) {
                return Paths.get(IndexDefinitionConfig.this.getPath(), fileName);
            }

            @Override
            public Charset getCharset() {
                return charset;
            }
        };
    }
}
