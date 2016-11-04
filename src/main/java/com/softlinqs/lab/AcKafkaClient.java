package com.softlinqs.lab;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.*;
import java.util.Properties;
import java.util.concurrent.*;

public class AcKafkaClient implements HealthCheckable {

    public static final Logger LOGGER = LoggerFactory.getLogger(AcKafkaClient.class);

    private final AcConfig acConfig;
    private final KafkaProducer<String, String> producer;
    private final ExecutorService executorService;

    public AcKafkaClient(AcConfig acConfig) {
        this.producer = new KafkaProducer<>(kafkaProducerPropertiesFrom(acConfig));
        this.acConfig = acConfig;
        this.executorService = Executors.newFixedThreadPool(5);
    }

    public HealthcheckResult isHealthy() {

        String[] strings = acConfig.getKafkaBootstrapServers().split(":");
        try {
            connectivityCheck(strings[0], Integer.valueOf(strings[1]), 300);
        } catch (SocketTimeoutException e) {
            return new HealthcheckResult(false, "Socket time out");
        } catch (ConnectException e) {
            return new HealthcheckResult(false, "Connect exception");
        } catch (IOException e) {
            LOGGER.error("IO exception", e);
            return new HealthcheckResult(false, "IO exception");
        }

        Callable<HealthcheckResult> callable = new Callable<HealthcheckResult>() {
            @Override
            public HealthcheckResult call() throws Exception {

                LOGGER.info("Before sending message...");

                Future<RecordMetadata> future = producer.send(new ProducerRecord<>("test-topic", "value"));
                future.get(500, TimeUnit.MILLISECONDS);
                LOGGER.info("After ack received");
                return new HealthcheckResult(true, "");
            }
        };

        Future<HealthcheckResult> future = executorService.submit(callable);
        try {
            LOGGER.info("After callable submit");
            return future.get(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            return new HealthcheckResult(false, "Interrupted");
        } catch (ExecutionException e) {
            return new HealthcheckResult(false, e.getCause().getMessage());
        } catch (TimeoutException e) {
            return new HealthcheckResult(false, "Timed out");
        }
    }

    private void connectivityCheck(String host, int port, int timeout) throws IOException {
        Socket socket = new Socket();
        SocketAddress socketAddress = new InetSocketAddress(host, port);
        socket.connect(socketAddress, timeout);
    }

    private Properties kafkaProducerPropertiesFrom(AcConfig acConfig) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, acConfig.getKafkaBootstrapServers());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        return properties;
    }
}
