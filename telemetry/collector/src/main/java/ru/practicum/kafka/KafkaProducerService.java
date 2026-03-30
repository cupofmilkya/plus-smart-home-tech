package ru.practicum.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaProducerService {
    private final KafkaTemplate<String, SensorEventAvro> sensorKafkaTemplate;
    private final KafkaTemplate<String, HubEventAvro> hubKafkaTemplate;

    public void sendSensorEvent(SensorEventAvro event) {
        log.debug("Preparing to send sensor event: hubId={}, timestamp={}",
                event.getHubId(), event.getTimestamp());

        CompletableFuture<SendResult<String, SensorEventAvro>> future =
                sensorKafkaTemplate.send("telemetry.sensors.v1", null, event);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Sensor event sent successfully: topic={}, partition={}, offset={}, hubId={}",
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset(),
                        event.getHubId());
            } else {
                log.error("Failed to send sensor event: hubId={}, timestamp={}",
                        event.getHubId(), event.getTimestamp(), ex);
            }
        });
    }

    public void sendHubEvent(HubEventAvro event) {
        log.debug("Preparing to send hub event: hubId={}, timestamp={}",
                event.getHubId(), event.getTimestamp());

        CompletableFuture<SendResult<String, HubEventAvro>> future =
                hubKafkaTemplate.send("telemetry.hubs.v1", null, event);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Hub event sent successfully: topic={}, partition={}, offset={}, hubId={}",
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset(),
                        event.getHubId());
            } else {
                log.error("Failed to send hub event: hubId={}, timestamp={}",
                        event.getHubId(), event.getTimestamp(), ex);
            }
        });
    }
}