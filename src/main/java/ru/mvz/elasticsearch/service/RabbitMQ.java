package ru.mvz.elasticsearch.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Delivery;
import lombok.Getter;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.*;
import ru.mvz.elasticsearch.config.RabbitMQConfig;

import java.io.IOException;
import java.time.Duration;

/**
 * Класс реализует методы
 *
 * @author  Валентин Морозов
 * @since   1.0
 */
@Service
@Getter
public class RabbitMQ implements ReactiveQueue {

    final private RabbitMQConfig rabbitMQConfig;

    final private Sender rabbitMQSender;

    final private ObjectMapper objectMapper;

    RabbitMQ(Sender rabbitMQSender, RabbitMQConfig rabbitMQConfig, ObjectMapper objectMapper) {
        this.rabbitMQConfig = rabbitMQConfig;
        this.rabbitMQSender = rabbitMQSender;
        this.objectMapper = objectMapper;
    }

    @Override
    public Flux<Delivery> inboundFlux() {
        return RabbitFlux
                .createReceiver()
                .consumeAutoAck(rabbitMQConfig.getQueue(), new ConsumeOptions()
                        .exceptionHandler(new ExceptionHandlers.RetryAcknowledgmentExceptionHandler(
                                Duration.ofSeconds(20), Duration.ofMillis(500),
                                ExceptionHandlers.CONNECTION_RECOVERY_PREDICATE
                        ))
                );
    }

    @Override
    public void send(String msg) {
        Mono<OutboundMessage> outboundMono = Mono.just(new OutboundMessage(
                "amq.direct",
                rabbitMQConfig.getRoutingKey(),
                msg.getBytes()
        ));
        rabbitMQSender.sendWithPublishConfirms(outboundMono)
                .subscribe(outboundMessageResult -> {
                    if (outboundMessageResult.isAck()) {
// TODO

                    }
                });

    }

    @Override
    public void sendIndexEvent(IndexEvent indexEvent) throws JsonProcessingException {
        send(objectMapper.writeValueAsString(indexEvent));
    }

    @Override
    public IndexEvent msg2IndexEvent(Delivery message) {
        return castMessage(message, IndexEvent.class);
    }

    @Override
    public  <T> T castMessage(Delivery message, Class<T> clazz) {
        try {
            return objectMapper.readValue(message.getBody(), clazz);
        } catch (IOException e) {
//   TODO         log.warn("Can't deserialize payload.", e);
            return null;
        }
    }

}
