package ru.mvz.elasticsearch.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.rabbitmq.client.Delivery;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.ExchangeFilterFunction;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ParallelFlux;
import reactor.core.scheduler.Schedulers;
import reactor.rabbitmq.*;
import reactor.util.function.Tuple2;
import reactor.util.retry.Retry;
import ru.mvz.elasticsearch.config.AppConfig;
import ru.mvz.elasticsearch.config.RabbitMQConfig;
import ru.mvz.elasticsearch.domain.MongoElasticIndex;
import ru.mvz.elasticsearch.repository.FileStorage;
import ru.mvz.elasticsearch.repository.ReactorRepositoryMongoDB;
import ru.mvz.elasticsearch.util.ConvertDataException;
import ru.mvz.elasticsearch.util.IllegalObjectIdException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.lang.Thread.sleep;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static ru.mvz.elasticsearch.util.Package.*;

/**
 * Класс реализует методы индексации данных MongoDB в ElasticSearch
 *
 * @author  Валентин Морозов
 * @since   1.0
 */
@Service
@Getter
public class Indexer {
    private static final Logger logger = LoggerFactory.getLogger(Indexer.class);

    final private ReactorRepositoryMongoDB reactorRepositoryMongoDB;

    final private MongoElasticIndexService mongoElasticIndexService;

    final private WebClient webClientElastic;

    final private RabbitMQConfig rabbitMQConfig;

    final private Receiver rabbitMQReceiver;

    final private RabbitMQ rabbitMQ;

    final private FileStorage fileStorage;

    final private AppConfig appConfig;

    final private AtomicLong sendRequest = new AtomicLong();

    final private AtomicLong receiveResponse = new AtomicLong();

    private Task rabbitMQTask;

    final private Set<Object> waitingForResponse = ConcurrentHashMap.newKeySet();

    /**
     * Список активных задач индексатора
     */
    final private List<Task> activeTasks = new ArrayList<>();

    public Indexer(ReactorRepositoryMongoDB reactorRepositoryMongoDB,
                   MongoElasticIndexService mongoElasticIndexService,
                   @Qualifier("elastic") WebClient.Builder webClientElastic,
                   RabbitMQConfig rabbitMQConfig,
                   Receiver rabbitMQReceiver,
                   RabbitMQ rabbitMQ,
                   FileStorage fileStorage,
                   AppConfig appConfig) {
        this.reactorRepositoryMongoDB = reactorRepositoryMongoDB;
        this.mongoElasticIndexService = mongoElasticIndexService;
        this.webClientElastic = webClientElastic
                .filter(onRequest())
                .filter(onResponse())
                .build();
        this.rabbitMQConfig = rabbitMQConfig;
        this.rabbitMQReceiver = rabbitMQReceiver;
        this.rabbitMQ = rabbitMQ;
        this.fileStorage = fileStorage;
        this.appConfig = appConfig;

    }

    @EventListener(ApplicationReadyEvent.class)
    public void runAfterStartup() {
        startRabbitMQTask();
    }

    private void startRabbitMQTask() {

        Flux<Delivery> inboundFlux = RabbitFlux
                .createReceiver()
                .consumeAutoAck(rabbitMQConfig.getQueue(), new ConsumeOptions()
                        .exceptionHandler(new ExceptionHandlers.RetryAcknowledgmentExceptionHandler(
                                Duration.ofSeconds(20), Duration.ofMillis(500),
                                ExceptionHandlers.CONNECTION_RECOVERY_PREDICATE
                        ))
                );
        rabbitMQTask = new Task(null);
        ParallelFlux dataEventsFlux = inboundFlux
                .parallel(appConfig.getIndexParallelism())
                .runOn(Schedulers.boundedElastic())
                .map(msg -> {
                    RabbitMQ.IndexEvent indexEvent = rabbitMQ.msg2IndexEvent(msg);
                    try {
                        return CreateIndexItem(indexEvent);
                    } catch (IllegalObjectIdException | IOException | ConvertDataException e) {
                        logger.error("{} For message: {}", String.join(", ",throwable2ListMessage(e)),
                                new String(msg.getBody(), StandardCharsets.UTF_8));
                    }
                    return new IndexItem(null, null, null);
                })
                .filter(e -> nonNull(e.getAction()))
                .flatMap(item ->
                        Flux.zip("delete".equals(item.getAction())
                                                ? Flux.just(new Document().append("_id", item.getIdDocument().get("_id")))
                                                : reactorRepositoryMongoDB.find(
                                                item.getMongoElasticIndex().getCollection(),
                                                item.getIdDocument(),
                                                item.getMongoElasticIndex().getProjection()),
                                        Flux.just(item))
                                .map(d -> new EventDocument(d.getT2().getAction(),
                                        d.getT1(),
                                        d.getT2().getMongoElasticIndex()))
                                .parallel(appConfig.getIndexParallelism())
                                .runOn(Schedulers.boundedElastic()));
        rabbitMQTask.setDispose(processingData(EventDocument::getAction,
                EventDocument::getDocument,
                EventDocument::getMongoElasticIndex,
                fileStorage.pullFileContents(),
                rabbitMQTask)
                .apply(dataEventsFlux));
        rabbitMQTask.setStartDate(new Date());
        addTask(rabbitMQTask);
    }

    public Mono<Document> sendIndexEvent(RabbitMQ.IndexEvent indexEvent)
            throws IOException, ConvertDataException, NotFoundIndexDefinitionException, IllegalObjectIdException {
        /*
        Проверка на наличие описания индекса
         */
        mongoElasticIndexService.getWithException(indexEvent.getIndexName(), indexEvent.getIndexType());
        /*
        Проверка на корректность _id документа
         */
        CreateIndexItem(indexEvent);

        rabbitMQ.sendIndexEvent(indexEvent);

        return Mono.just(new Document().append("id", indexEvent.getId())
                .append("index", indexEvent.getIndexName())
                .append("type", isNull(indexEvent.getIndexType()) ? "" : indexEvent.getIndexType()));
    }

    public Mono<Document> refresh(String indexName, String indexType) throws IOException, ConvertDataException, NotFoundIndexDefinitionException {

        Document result = new Document();
        MongoElasticIndex mongoElasticIndex = mongoElasticIndexService.getWithException(indexName, indexType);

        if(nonNull(mongoElasticIndex)) {
            Task task = new Task(mongoElasticIndex);
            ParallelFlux dataEventsFlux = reactorRepositoryMongoDB
                    .findAll(mongoElasticIndex.getCollection(), mongoElasticIndex.getProjection())

                    .parallel(appConfig.getIndexParallelism())
                    .runOn(Schedulers.boundedElastic());

            task.setDispose(processingData((p) -> "index",
                    (p) -> (Document)p,
                    (p) -> mongoElasticIndex,
                    Flux.just(),
                    task)
                    .apply(dataEventsFlux));
            task.setStartDate(new Date());
            addTask(task);
            result.append("Index refresh", new Document()
                    .append("Index", "name: " + indexName + (Objects.isNull(indexType) ? "" : "type: " + indexType))
                    .append("started", task.getStartDate().toString())
            );
        }
        return Mono.just(result);
    }

    private <T> Function<ParallelFlux<T>, Disposable>
        processingData(Function<T, String> getAction,
                       Function<T, Document> getDocument,
                       Function<T, MongoElasticIndex> getMongoElasticIndex,
                       Flux<String> mergeFlux,
                       Task task) {
        return (ParallelFlux<T> events) -> events
            .transform(joinData(getDocument, getMongoElasticIndex))    // Добавление данных к исходному документу из присоединяемых коллекций
            .transform(document2ElasticJson(getAction, getDocument, getMongoElasticIndex)) // Генерация данных для передачи в ElasticSearch

            .sequential()
            .transform(grouping(task))      // Агрегирование данных для _bulk
            .mergeWith(mergeFlux)
            .transform(postBulk(task))          // Отправка запросов в ElasticSearch
            .subscribeOn(Schedulers.single())
            .doOnNext(testAliveResponses(task))
            .doOnSubscribe(p-> p.request(appConfig.getMaxSizeBuffer() * 2))

            .doOnComplete(() -> { logger.info("Start: {} End: {} read {} write {}",
                    formatDate(task.getStartDate()),
                    formatDate(new Date()),
                    task.getDocumentsRead(),
                    task.getIndexesWrite(), getMaxProcessingRequest());
                fileStorage.writeCollection2Files(waitingForResponse);
                removeTask(task);
            })

            .subscribe(
                    p -> {
                        if(isNull(task.getMongoElasticIndex())) { // Если задача не переиндексация
                            waitingForResponse.remove(p.getT1());
                        }
                        int count = Optional.ofNullable(p.getT2().get("items", List.class))
                                .map(List::size)
                                .orElse(0);
                        task.addIndexesWrite(count);
                    },
                    e -> {
                        if(task != rabbitMQTask)removeTask(task);
                        fileStorage.writeCollection2Files(waitingForResponse);
                        logger.error("Error: {}", e.getMessage());
                    }
            );
    }

    /**
     * Создает функциональные объект, добавляющий к документу данные из присоединяемых коллекций
     * В качестве параметра функциональный объект принимает поток {@code ParallelFlux<Document>}
     * и возвращает поток {@code ParallelFlux<Document>}
     *
     * @param getMongoElasticIndex функциональный объект, возвращающи описания индекса
     * @return функциональный объект, модифицирующий документ
     */
    private <T> Function<ParallelFlux<T>, ParallelFlux<T>>
        joinData(Function<T, Document> getDocument,
                Function<T, MongoElasticIndex> getMongoElasticIndex) {
        return (ParallelFlux<T> documents) ->
            documents.flatMap(p -> {
                if(getDocument.apply(p).size() == 1) {
                    return Flux.just(p);
                }
                return
                    Flux.fromIterable(getMongoElasticIndex.apply((T) p).getJoinConditions(getDocument.apply(p)))
                            .flatMap(it -> Flux.zip(Flux.just(it.getCollection().getJoinedFieldName()),
                                    reactorRepositoryMongoDB.find(getMongoElasticIndex.apply((T) p).getCollection(), it.getCondition(),
                                            it.getCollection().getProjection())))
                            .reduce(p, (acc, t) -> {
                                getDocument.apply(acc).put(t.getT1(), t.getT2());
                                return acc;
                            });
                    }
            );
    }
    /**
     * Создает функциональные объект, генерирующий данные для отправки в Elastic Search
     * В качестве параметра функциональный объект принимает поток {@code ParallelFlux<Document>}
     * и возвращает поток {@code ParallelFlux<String>}
     *
     * @param getMongoElasticIndex функциональный объект, возвращающи описания индекса
     * @return функциональный объект, генерирующий данные
     */
    private <T> Function<ParallelFlux<T>, ParallelFlux<String>>
        document2ElasticJson(
                Function<T, String> getAction,
                Function<T, Document> getDocument,
                Function<T, MongoElasticIndex> getMongoElasticIndex) {
        return (ParallelFlux<T> items) -> items.map(item -> {
            String elasticSend;
            try {
                Document document = getDocument.apply(item);
                MongoElasticIndex mongoElasticIndex = getMongoElasticIndex.apply(item);
                elasticSend = "delete".equals(getAction.apply(item))
                        ? mongoElasticIndex.deleteBuild(document)
                        : mongoElasticIndex.indexBuild(document);
            } catch (ConvertDataException e) {
                throw new RuntimeException(e);
            } catch (JsonProcessingException e) {
                throw new UncheckedIOException(e);
            }
            return elasticSend;
        });
    }
    /**
     * Создает функциональные объект, группирующий данные для bulk-запроса в ElasticSearch
     * В качестве параметра функциональный объект принимает поток {@code Flux<String>}
     * и возвращает поток {@code Flux<String>}
     *
     * @return функциональный объект, группирующий данные
     */
    Function<Flux<String>, Flux<String>> grouping(Task task) {
        return (Flux<String> source) -> source
                .bufferTimeout(appConfig.getMaxSizeBuffer(),
                        Duration.ofMillis(appConfig.getMaxDurationBuffer()))
                .doOnNext(p -> task.addDocumentsRead(p.size()))
                .map(p -> String.join("\n", p)
                );
    }
    /**
     * Создает функциональные объект, отправляющий HTTP-запросы к ElasticSearch
     * В качестве параметра функциональный объект принимает поток {@code Flux<String>}
     * и возвращает поток {@code Flux<Document>}
     *
     * @return функциональный объект, выполнняющий HTTP-запросы к ElasticSearch
     */
    public Function<Flux<String>, Flux<Tuple2<String, Document>>> postBulk(Task task) {
        return (Flux<String> source) -> source
            .flatMap(buffer -> {
                if(isNull(task.getMongoElasticIndex())) { // Если задача не переиндексация
                    waitingForResponse.add(buffer);
                }
                return Flux.zip(Flux.just(buffer),
                    webClientElastic.post()
                        .uri("/_bulk")
                        .body(BodyInserters.fromValue(buffer))
                        .retrieve()
                        .onStatus(httpStatus -> httpStatus.equals(HttpStatus.TOO_MANY_REQUESTS),
                                response -> Mono.error(new HttpServiceException("System is overloaded",
                                        response.rawStatusCode())))
                        .onStatus(httpStatus -> httpStatus.is4xxClientError() && !httpStatus.equals(HttpStatus.TOO_MANY_REQUESTS),
                                response -> Mono.error(new RuntimeException("API not found")))
                        .onStatus(HttpStatus::is5xxServerError,
                                response -> Mono.error(new HttpServiceException("Server is not responding",
                                        response.rawStatusCode())))
                        .bodyToFlux(Document.class)
                        .retryWhen(Retry.backoff(appConfig.getWebClientRetryMaxAttempts(),
                                    Duration.ofSeconds(appConfig.getWebClientRetryMinBackoff()))
                            .filter(throwable -> throwable instanceof HttpServiceException)
                            .onRetryExhaustedThrow((retryBackoffSpec, retrySignal) -> {
                                throw new HttpServiceException("External Service failed to process after max retries",
                                        HttpStatus.SERVICE_UNAVAILABLE.value());
                            }))
                );
            });

    }

    Consumer<Object> testAliveResponses(Task task) {
        return (Object object) -> {
            if (getProcessingRequest() > getMaxProcessingRequest() * 2) {
                task.dispose();
            }
        };
    }

    private ExchangeFilterFunction onRequest() {
        return ExchangeFilterFunction.ofRequestProcessor(clientRequest -> {
            addSendRequest();
            int sleepCycleCount = 0;
            while (getProcessingRequest() > getMaxProcessingRequest()) {
                try {
                    logger.info("Sleep: {} ProcessingRequest reached {} (MaxProcessingRequest {})", getSleepOverRequest(),
                            getProcessingRequest() - 1, getMaxProcessingRequest());
                    sleep(getSleepOverRequest());
                    if (sleepCycleCount++ > appConfig.getSleepCycleCountMax()) {
                        break;
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            logger.info("Request: {} {}", clientRequest.method(), clientRequest.url());
            return Mono.just(clientRequest);
        });
    }

    private ExchangeFilterFunction onResponse() {
        return ExchangeFilterFunction.ofResponseProcessor(clientResponse -> {
            addReceiveResponse();
            logger.info("Response Status {}", clientResponse.statusCode());
            return Mono.just(clientResponse);
        });
    }

    public long addSendRequest() {
        return sendRequest.incrementAndGet();
    }

    public long addReceiveResponse() {
        return receiveResponse.incrementAndGet();
    }

    public long getProcessingRequest() {
        return sendRequest.get() - receiveResponse.get();
    }

    public int getMaxProcessingRequest() {
        return appConfig.getMaxProcessingRequest();
    }
    public int getSleepOverRequest() {
        return appConfig.getSleepOverRequest();
    }

    private void addTask(Task task) {
        synchronized (activeTasks) {
            activeTasks.add(task);
        }
    }

    private void removeTask(Task task) {
        synchronized (activeTasks) {
            activeTasks.remove(task);
        }
    }
    
    @Getter
    @AllArgsConstructor
    static private class IndexItem {

        final private String action;

        final private Document idDocument;

        final private MongoElasticIndex mongoElasticIndex;
    }

    IndexItem CreateIndexItem(RabbitMQ.IndexEvent indexEvent)
            throws IllegalObjectIdException, IOException, ConvertDataException {

        return new IndexItem(indexEvent.getAction(), idDocument(indexEvent.getId()),
                mongoElasticIndexService.get(indexEvent.getIndexName(), indexEvent.getIndexType()));

    }
    
    @Getter
    @AllArgsConstructor
    static private class EventDocument extends Document {

        final private String action;

        final private Document document;

        final private MongoElasticIndex mongoElasticIndex;

        static public String getAction(Object p) {
            return isNull(p) ? null : ((EventDocument)p).getAction();
        }

        static public Document getDocument(Object p) {
            return isNull(p) ? null : ((EventDocument)p).getDocument();
        }

        static public MongoElasticIndex getMongoElasticIndex(Object p) {
            return isNull(p) ? null : ((EventDocument)p).getMongoElasticIndex();
        }

    }

    /**
     * Класс содержит данные о выполняемой задаче индексации
     *
     * @author  Валентин Морозов
     * @since   1.0
     */
    @Getter
    @Setter
    static public class Task {

        private Disposable dispose;

        final private MongoElasticIndex mongoElasticIndex;

        private Date startDate;

        private Date lastActivity;

        private int documentsRead;

        private int indexesWrite;

        synchronized public int addDocumentsRead(int count) {
            lastActivity = new Date();
            return (documentsRead += count);
        }

        synchronized public int addIndexesWrite(int count) {
            lastActivity = new Date();
            return (indexesWrite += count);
        }

        Task(MongoElasticIndex mongoElasticIndex) {
            this.mongoElasticIndex = mongoElasticIndex;
        }

        boolean dispose() {
            if(nonNull(dispose)) {
                getDispose().dispose();
                return true;
            }
            return false;
        }

    }

    static public class HttpServiceException extends RuntimeException {
        int statusCode;
        public HttpServiceException(String message, int statusCode) {
            super(message);
            this.statusCode = statusCode;
        }
    }

}
