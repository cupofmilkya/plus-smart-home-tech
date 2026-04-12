package ru.yandex.practicum.consumers;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.client.HubRouterClient;
import ru.yandex.practicum.entity.Scenario;
import ru.yandex.practicum.entity.Snapshot;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.repository.ScenarioRepository;
import ru.yandex.practicum.service.ScenarioExecutor;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Slf4j
@Component
@RequiredArgsConstructor
public class SnapshotProcessor {

    private final ScenarioRepository scenarioRepository;
    private final ScenarioExecutor scenarioExecutor;
    private final HubRouterClient hubRouterClient;

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    public void start() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", "analyzer-snapshot-group");
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "latest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of("telemetry.snapshots.v1"));

            while (true) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, byte[]> record : records) {
                    processSnapshot(record.value());
                }
                consumer.commitSync();
            }
        }
    }

    private void processSnapshot(byte[] snapshotBytes) {
        try {
            Snapshot snapshot = deserializeSnapshot(snapshotBytes);

            List<Scenario> scenarios = scenarioRepository.findByHubId(snapshot.getHubId());

            for (Scenario scenario : scenarios) {
                List<DeviceActionProto> actions = scenarioExecutor.evaluateScenario(scenario, snapshot);
                if (!actions.isEmpty()) {
                    for (DeviceActionProto action : actions) {
                        hubRouterClient.sendAction(
                                snapshot.getHubId(),
                                scenario.getName(),
                                action,
                                com.google.protobuf.Timestamp.newBuilder()
                                        .setSeconds(System.currentTimeMillis() / 1000)
                                        .build()
                        );
                    }
                }
            }
        } catch (Exception e) {
            log.error("Error processing snapshot", e);
        }
    }

    private Snapshot deserializeSnapshot(byte[] bytes) {
        // TODO: десериализация Avro снапшота
        return Snapshot.builder()
                .hubId("test-hub")
                .sensorValues(Map.of())
                .build();
    }
}