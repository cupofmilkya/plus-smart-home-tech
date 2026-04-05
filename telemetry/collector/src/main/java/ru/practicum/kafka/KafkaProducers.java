package ru.practicum.kafka;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Component;
import ru.practicum.kafka.serialization.CustomHubEventSerializer;
import ru.practicum.kafka.serialization.CustomSensorEventSerializer;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

import java.util.Properties;

@Component
public class KafkaProducers {

    private KafkaProducer<String, SensorEventAvro> sensorProducer;
    private KafkaProducer<String, HubEventAvro> hubProducer;

    @PostConstruct
    public void init() {
        // Sensor
        Properties sensorProps = new Properties();
        sensorProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        sensorProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        sensorProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CustomSensorEventSerializer.class);

        sensorProducer = new KafkaProducer<>(sensorProps);

        // Hub
        Properties hubProps = new Properties();
        hubProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        hubProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        hubProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CustomHubEventSerializer.class);

        hubProducer = new KafkaProducer<>(hubProps);
    }

    public KafkaProducer<String, SensorEventAvro> sensor() {
        return sensorProducer;
    }

    public KafkaProducer<String, HubEventAvro> hub() {
        return hubProducer;
    }

    @PreDestroy
    public void close() {
        sensorProducer.close();
        hubProducer.close();
    }
}