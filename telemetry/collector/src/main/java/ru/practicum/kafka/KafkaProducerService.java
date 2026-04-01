package ru.practicum.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaProducerService {

    private final KafkaProducers producers;

    public void sendSensorEvent(SensorEventAvro event) {
        String key = event.getHubId().toString();

        ProducerRecord<String, SensorEventAvro> record =
                new ProducerRecord<>("telemetry.sensors.v1", key, event);

        producers.sensor().send(record, (metadata, ex) -> {
            if (ex == null) {
                log.info("Sensor event sent: topic={}, partition={}, offset={}",
                        metadata.topic(),
                        metadata.partition(),
                        metadata.offset());
            } else {
                log.error("Failed to send sensor event", ex);
            }
        });
    }

    public void sendHubEvent(HubEventAvro event) {
        String key = event.getHubId().toString();

        ProducerRecord<String, HubEventAvro> record =
                new ProducerRecord<>("telemetry.hubs.v1", key, event);

        producers.hub().send(record, (metadata, ex) -> {
            if (ex == null) {
                log.info("Hub event sent: topic={}, partition={}, offset={}",
                        metadata.topic(),
                        metadata.partition(),
                        metadata.offset());
            } else {
                log.error("Failed to send hub event", ex);
            }
        });
    }
}