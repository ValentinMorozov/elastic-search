package ru.mvz.elasticsearch.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Delivery;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.OutboundMessage;
import reactor.rabbitmq.Sender;
import ru.mvz.elasticsearch.config.RabbitMQConfig;

import java.io.IOException;

/**
 * Класс реализует методы
 *
 * @author  Валентин Морозов
 * @since   1.0
 */
@Service
@Getter
public class RabbitMQ {

    final private RabbitMQConfig rabbitMQConfig;

    final private Sender rabbitMQSender;

    final private ObjectMapper objectMapper;

    RabbitMQ(Sender rabbitMQSender, RabbitMQConfig rabbitMQConfig, ObjectMapper objectMapper) {
        this.rabbitMQConfig = rabbitMQConfig;
        this.rabbitMQSender = rabbitMQSender;
        this.objectMapper = objectMapper;
    }
    public void send(String msg) {
        Mono<OutboundMessage> outboundMono = Mono.just(new OutboundMessage(
                "amq.direct",
                rabbitMQConfig.getRoutingKey(),
                msg.getBytes()
        ));
        rabbitMQSender.sendWithPublishConfirms(outboundMono)
                .subscribe(outboundMessageResult -> {
                    if (outboundMessageResult.isAck()) {
// ???

                    }
                });

    }

    public void sendIndexEvent(IndexEvent indexEvent) throws JsonProcessingException {
        send(objectMapper.writeValueAsString(indexEvent));
    }

    public IndexEvent msg2IndexEvent(Delivery message) {
        return castMessage(message, IndexEvent.class);
    }

    public  <T> T castMessage(Delivery message, Class<T> clazz) {
        try {
            return objectMapper.readValue(message.getBody(), clazz);
        } catch (IOException e) {
//            log.warn("Can't deserialize payload.", e);
            return null;
        }
    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    static public class IndexEvent {

        private String action;

        private String id;

        private String indexName;

        private String indexType;

    }
}
