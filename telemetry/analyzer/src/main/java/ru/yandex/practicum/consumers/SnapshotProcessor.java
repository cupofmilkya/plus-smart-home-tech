package ru.yandex.practicum.consumers;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.entity.Snapshot;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.service.SnapshotProcessingService;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Slf4j
@Component
@RequiredArgsConstructor
public class SnapshotProcessor {

    private final SnapshotProcessingService processingService;

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    private volatile boolean running = true;
    private KafkaConsumer<String, byte[]> consumer;

    public void start() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", "analyzer-snapshot-group");
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "latest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of("telemetry.snapshots.v1"));
        log.info("SnapshotProcessor started, subscribed to telemetry.snapshots.v1");

        try {
            while (running) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(1000));

                if (records.isEmpty()) {
                    continue;
                }

                boolean allProcessedSuccessfully = true;

                for (ConsumerRecord<String, byte[]> record : records) {
                    try {
                        Snapshot snapshot = deserializeSnapshot(record.value());
                        log.info("Processing snapshot for hubId: {}, sensors: {}, offset: {}",
                                snapshot.getHubId(), snapshot.getSensorValues().keySet(), record.offset());
                        processingService.processSnapshot(snapshot);
                    } catch (Exception e) {
                        log.error("Error processing snapshot at offset: {}, partition: {}",
                                record.offset(), record.partition(), e);
                        allProcessedSuccessfully = false;
                        break;
                    }
                }

                if (allProcessedSuccessfully) {
                    consumer.commitSync();
                    log.debug("Successfully committed offsets for {} records", records.count());
                } else {
                    log.warn("Skipping commit due to processing error. Will reprocess the same batch on next poll");
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        } catch (WakeupException e) {
            log.info("WakeupException caught, shutting down SnapshotProcessor");
        } catch (Exception e) {
            log.error("Error in snapshot consumer loop", e);
        } finally {
            if (consumer != null) {
                consumer.close();
                log.info("SnapshotProcessor consumer closed");
            }
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
                    String sensorIdStr = sensorId.toString();
                    Object value = extractSensorValue(sensorState);
                    if (value != null) {
                        sensorValues.put(sensorIdStr, value);
                    }
                });
            }

            String hubIdStr = avroSnapshot.getHubId().toString();

            return Snapshot.builder()
                    .hubId(hubIdStr)
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
        if (consumer != null) {
            consumer.wakeup();
        }
        log.info("SnapshotProcessor stopped");
    }
}