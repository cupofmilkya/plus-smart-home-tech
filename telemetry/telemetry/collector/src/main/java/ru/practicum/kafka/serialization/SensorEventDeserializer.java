package ru.practicum.kafka.serialization;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;

@Slf4j
public class SensorEventDeserializer implements Deserializer<SensorEventAvro> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        log.debug("Configuring SensorEventDeserializer");
    }

    @Override
    public SensorEventAvro deserialize(String topic, byte[] data) {
        if (data == null) {
            log.warn("Received null data for topic: {}", topic);
            return null;
        }

        try {
            log.debug("Deserializing {} bytes from topic: {}", data.length, topic);

            DatumReader<SensorEventAvro> reader = new SpecificDatumReader<>(SensorEventAvro.getClassSchema());
            ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
            Decoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);

            SensorEventAvro event = reader.read(null, decoder);

            log.info("=== Deserialized SensorEvent ===");
            log.info("id: {}", event.getId());
            log.info("hubId: {}", event.getHubId());
            log.info("timestamp: {}", event.getTimestamp());

            if (event.getPayload() != null) {
                log.info("payload type: {}", event.getPayload().getClass().getSimpleName());
                log.info("payload: {}", event.getPayload());
            } else {
                log.warn("payload is null");
            }

            return event;

        } catch (IOException e) {
            log.error("Error deserializing SensorEventAvro", e);
            throw new SerializationException("Error deserializing SensorEventAvro", e);
        }
    }

    @Override
    public void close() {
        log.debug("Closing SensorEventDeserializer");
    }
}