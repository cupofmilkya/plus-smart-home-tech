package ru.yandex.practicum.consumers;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
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
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.HashMap;

@Slf4j
@Component
@RequiredArgsConstructor
public class SnapshotProcessor {

    private final ScenarioRepository scenarioRepository;
    private final ScenarioExecutor scenarioExecutor;
    private final HubRouterClient hubRouterClient;

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    private volatile boolean running = true;

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

            while (running) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, byte[]> record : records) {
                    processSnapshot(record.value());
                }
                consumer.commitSync();
            }
        } catch (Exception e) {
            log.error("Error in snapshot consumer loop", e);
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
                                action.getSensorId(),
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
        try {
            SpecificDatumReader<SensorsSnapshotAvro> reader = new SpecificDatumReader<>(SensorsSnapshotAvro.class);
            Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
            SensorsSnapshotAvro avroSnapshot = reader.read(null, decoder);

            Map<String, Object> sensorValues = new HashMap<>();

            if (avroSnapshot.getSensorsState() != null) {
                avroSnapshot.getSensorsState().forEach((sensorId, sensorState) -> {
                    Object value = extractSensorValue(sensorState);
                    if (value != null) {
                        sensorValues.put((String) sensorId, value);
                    }
                });
            }

            return Snapshot.builder()
                    .hubId(avroSnapshot.getHubId().toString())
                    .sensorValues(sensorValues)
                    .build();
        } catch (Exception e) {
            log.error("Failed to deserialize Avro snapshot", e);
            throw new RuntimeException("Failed to deserialize snapshot", e);
        }
    }

    private Object extractSensorValue(SensorStateAvro sensorState) {
        if (sensorState.getData() == null) {
            return null;
        }

        Object data = sensorState.getData();
        String dataType = data.getClass().getSimpleName();

        switch (dataType) {
            case "MotionSensorAvro":
                try {
                    return data.getClass().getMethod("getMotion").invoke(data);
                } catch (Exception e) {
                    log.error("Failed to get motion value", e);
                }
                break;

            case "TemperatureSensorAvro":
                try {
                    return data.getClass().getMethod("getTemperatureC").invoke(data);
                } catch (Exception e) {
                    log.error("Failed to get temperature value", e);
                }
                break;

            case "LightSensorAvro":
                try {
                    return data.getClass().getMethod("getLuminosity").invoke(data);
                } catch (Exception e) {
                    log.error("Failed to get luminosity value", e);
                }
                break;

            case "ClimateSensorAvro":
                try {
                    return data.getClass().getMethod("getTemperatureC").invoke(data);
                } catch (Exception e) {
                    log.error("Failed to get climate temperature value", e);
                }
                break;

            case "SwitchSensorAvro":
                try {
                    return data.getClass().getMethod("getState").invoke(data);
                } catch (Exception e) {
                    log.error("Failed to get switch state value", e);
                }
                break;
        }

        return null;
    }

    public void stop() {
        running = false;
        log.info("SnapshotProcessor stopped");
    }
}