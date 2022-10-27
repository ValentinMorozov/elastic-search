package ru.mvz.elasticsearch.config;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import reactor.core.scheduler.Schedulers;
import reactor.rabbitmq.*;

@Getter
@Setter
@NoArgsConstructor
@ConfigurationProperties(prefix = "rabbitmq")
public class RabbitMQConfig {

    private String host;

    private String queue;

    private String user;

    private String password;

    private String routingKey;

    @Getter(lazy=true)
    private final Utils.ExceptionFunction<ConnectionFactory, ? extends Connection>
            connectionSupplier = createConnectionSupplier();

    final private ConnectionFactory connectionFactory() {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.useNio();
        return connectionFactory;
    }


    Utils.ExceptionFunction<ConnectionFactory, ? extends Connection> createConnectionSupplier() {
        return Utils.singleConnectionSupplier(
                connectionFactory(), cf -> cf.newConnection(new Address[] { new Address(host) },
                        "reactive-sender-receiver"));
    }

    @Bean
    Sender sender() {
        return RabbitFlux.createSender(
                new SenderOptions()
                        .connectionSupplier(getConnectionSupplier())
                        .resourceManagementScheduler(Schedulers.boundedElastic())
        );
    }

    @Bean
    Receiver receiver() {
        return RabbitFlux.createReceiver(
                new ReceiverOptions()
                        .connectionSupplier(getConnectionSupplier())
                        .connectionSubscriptionScheduler(Schedulers.boundedElastic())
        );
    }
}
