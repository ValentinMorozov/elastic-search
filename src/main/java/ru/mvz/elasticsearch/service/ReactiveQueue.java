package ru.mvz.elasticsearch.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.rabbitmq.client.Delivery;
import reactor.core.publisher.Flux;

public interface ReactiveQueue {
    Flux<Delivery> inboundFlux();

    void send(String msg);

    void sendIndexEvent(IndexEvent indexEvent) throws JsonProcessingException;

    IndexEvent msg2IndexEvent(Delivery message);

    <T> T castMessage(Delivery message, Class<T> clazz);

}
