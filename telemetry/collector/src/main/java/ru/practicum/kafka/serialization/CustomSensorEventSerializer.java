package ru.practicum.kafka.serialization;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

@Slf4j
public class CustomSensorEventSerializer implements Serializer<SensorEventAvro> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        log.debug("Configuring CustomSensorEventSerializer");
    }

    @Override
    public byte[] serialize(String topic, SensorEventAvro data) {
        if (data == null) {
            log.warn("Attempting to serialize null SensorEventAvro");
            return null;
        }

        try {
            DatumWriter<SensorEventAvro> writer = new SpecificDatumWriter<>(SensorEventAvro.getClassSchema());
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);

            writer.write(data, encoder);
            encoder.flush();

            byte[] bytes = out.toByteArray();
            log.debug("Serialized SensorEventAvro: {} bytes", bytes.length);
            return bytes;

        } catch (IOException e) {
            log.error("Error serializing SensorEventAvro", e);
            throw new SerializationException("Error serializing SensorEventAvro", e);
        }
    }

    @Override
    public void close() {
        log.debug("Closing CustomSensorEventSerializer");
    }
}