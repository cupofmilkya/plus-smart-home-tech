package ru.practicum.kafka.serialization;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

@Slf4j
public class CustomHubEventSerializer implements Serializer<HubEventAvro> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        log.info("Configuring CustomHubEventSerializer, isKey: {}", isKey);
        if (configs != null) {
            log.debug("Configuration: {}", configs);
        }
    }

    @Override
    public byte[] serialize(String topic, HubEventAvro data) {
        if (data == null) {
            log.warn("Attempting to serialize null HubEventAvro for topic: {}", topic);
            return null;
        }

        log.info("=== Serializing HubEventAvro ===");
        log.info("Topic: {}", topic);
        log.info("HubId: {}", data.getHubId());
        log.info("Timestamp: {}", data.getTimestamp());

        if (data.getPayload() != null) {
            log.info("Payload type: {}", data.getPayload().getClass().getSimpleName());
        }

        try {
            DatumWriter<HubEventAvro> writer = new SpecificDatumWriter<>(HubEventAvro.getClassSchema());
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);

            writer.write(data, encoder);
            encoder.flush();

            byte[] bytes = out.toByteArray();
            log.info("Successfully serialized HubEventAvro: {} bytes", bytes.length);
            return bytes;

        } catch (IOException e) {
            log.error("Error serializing HubEventAvro", e);
            throw new SerializationException("Error serializing HubEventAvro", e);
        }
    }

    @Override
    public void close() {
        log.debug("Closing CustomHubEventSerializer");
    }
}