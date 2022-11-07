package ru.mvz.elasticsearch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.EventListener;
import reactor.core.publisher.Hooks;
import ru.mvz.elasticsearch.config.AppConfig;
import ru.mvz.elasticsearch.service.Indexer;

import static ru.mvz.elasticsearch.util.Package.*;

@SpringBootApplication
@ConfigurationPropertiesScan("ru.mvz.elasticsearch.config")
public class ElasticSearchApplication {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchApplication.class);

    final private Indexer indexer;

    final private AppConfig appConfig;

    static ApplicationContext applicationContext;

    public static void main(String[] args) {
        applicationContext = SpringApplication.run(ElasticSearchApplication.class, args);
    }

    ElasticSearchApplication (AppConfig appConfig,
                          Indexer indexer) {
        this.appConfig = appConfig;
        this.indexer = indexer;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void runAfterStartup() throws Exception {
        logger.info("Indexer running........");

        if(appConfig.isHooksOnErrorDropped()) {
            Hooks.onErrorDropped(e -> {
                        throwable2ListMessage(e).forEach(msg -> logger.error("Error: {}", msg));
// TODO ошибка запуска
//  SpringApplication.exit(applicationContext, () -> 0);
                    }
            );
        }
    }

}
