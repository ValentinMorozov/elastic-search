package ru.mvz.elasticsearch.config;

import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.MongoCredential;
import com.mongodb.reactivestreams.client.MongoDatabase;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.config.AbstractReactiveMongoConfiguration;
import org.springframework.data.mongodb.repository.config.EnableReactiveMongoRepositories;

import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;

import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

@Getter
@Setter
@NoArgsConstructor
@ConfigurationProperties(prefix = "mongo")
@EnableReactiveMongoRepositories(basePackages = "ru.mvz.elasticsearch.repository")
@Configuration
public class MongoConfig extends AbstractReactiveMongoConfiguration
{
    private String  host;

    @Value("${:27017}")
    private Integer port;

    private String databaseName;

    private String user;

    private String password;

    private MongoClientSettings getMongoClientSettings() {
        return  MongoClientSettings.builder()
                        .applyToClusterSettings(builder ->
                                builder.hosts(Arrays.asList(
                                        new ServerAddress(getHost(), getPort()))))
                        .credential(MongoCredential
                                .createCredential(getUser(),
                                        getDatabaseName(),
                                        getPassword().toCharArray()))
                        .build();
    }

    @Bean
    @Override
    public MongoClient reactiveMongoClient() {
        return MongoClients.create(getMongoClientSettings());
    }

    @Bean
    MongoDatabase mongoDatabase() {
        return reactiveMongoClient().getDatabase(getDatabaseName());
    }
}