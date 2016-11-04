package com.softlinqs.lab;

import info.batey.kafka.unit.KafkaUnitRule;
import org.junit.Rule;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AcKafkaClientTest {

    @Rule
    public KafkaUnitRule kafkaUnitRule = new KafkaUnitRule(2181, 9092);

    @Test
    public void isHealthy_ReturnsHealthyResult_WhenKafkaBrokerIsUpAndRunning() {
        //given
        AcConfig acConfig = mock(AcConfig.class);
        when(acConfig.getKafkaBootstrapServers()).thenReturn("localhost:9092");
        AcKafkaClient client = new AcKafkaClient(acConfig);

        //when
        HealthcheckResult result = client.isHealthy();

        //then
        assertThat(result.isHealthy()).isTrue();
    }

    @Test
    public void isHealthy_ReturnsUnhealthyResult_WhenKafkaBrokerIsNotRunning() {
        //given
        AcConfig acConfig = mock(AcConfig.class);
        when(acConfig.getKafkaBootstrapServers()).thenReturn("localhost:9095");
        AcKafkaClient client = new AcKafkaClient(acConfig);

        //when
        HealthcheckResult result = client.isHealthy();

        //then
        assertThat(result.isHealthy()).isFalse();
        assertThat(result.getMessage()).isEqualTo("Timed out");
    }

    @Test
    public void isHealthy_ReturnsUnhealthyResult_WhenFirewallPortIsNotOpen() {
        //given
        AcConfig acConfig = mock(AcConfig.class);
        when(acConfig.getKafkaBootstrapServers()).thenReturn("192.168.10.11:9095");
        AcKafkaClient client = new AcKafkaClient(acConfig);

        //when
        HealthcheckResult result = client.isHealthy();

        //then
        assertThat(result.isHealthy()).isFalse();
        assertThat(result.getMessage()).isEqualTo("Socket time out");
    }
}
