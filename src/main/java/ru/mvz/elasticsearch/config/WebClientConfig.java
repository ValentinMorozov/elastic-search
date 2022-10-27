package ru.mvz.elasticsearch.config;

import io.netty.channel.ChannelOption;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.handler.timeout.WriteTimeoutHandler;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.ExchangeFilterFunction;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;
import reactor.netty.tcp.TcpClient;

import java.util.concurrent.TimeUnit;

import static org.springframework.web.reactive.function.client.ExchangeFilterFunctions.basicAuthentication;

@Getter
@Setter
@NoArgsConstructor
@ConfigurationProperties(prefix = "elastic")
public class WebClientConfig {

    @Value("${:localhost}")
    private String  baseUrl;

    @Value("${:9200}")
    private Integer port;

    @Value("${:1000}")
    private Integer timeout;

    private String user;

    private String password;

    @Bean
    @Qualifier("elastic")
    public WebClient.Builder webClientWithTimeout() {
        final TcpClient tcpClient = TcpClient
                .create()
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, timeout)
                .doOnConnected(connection -> {
                    connection.addHandlerLast(new ReadTimeoutHandler(timeout, TimeUnit.MILLISECONDS));
                    connection.addHandlerLast(new WriteTimeoutHandler(timeout, TimeUnit.MILLISECONDS));
                });

        return WebClient.builder()
                .baseUrl(baseUrl +":" + port.toString())
                .filter(basicAuthentication(user, password))
                .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE);
    }

}
