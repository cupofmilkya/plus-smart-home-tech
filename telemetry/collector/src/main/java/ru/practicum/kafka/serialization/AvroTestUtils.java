package ru.practicum.kafka.serialization;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Base64;

@Slf4j
@Component
public class AvroTestUtils {

    public static void logSerializedData(SensorEventAvro event) {
        try {
            DatumWriter<SensorEventAvro> writer = new SpecificDatumWriter<>(SensorEventAvro.getClassSchema());
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            writer.write(event, encoder);
            encoder.flush();

            byte[] serialized = out.toByteArray();

            log.info("=== Serialized SensorEventAvro ===");
            log.info("Size: {} bytes", serialized.length);
            log.info("Hex: {}", bytesToHex(serialized));
            log.info("Base64: {}", Base64.getEncoder().encodeToString(serialized));

            DatumReader<SensorEventAvro> reader = new SpecificDatumReader<>(SensorEventAvro.getClassSchema());
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(serialized, null);
            SensorEventAvro deserialized = reader.read(null, decoder);

            log.info("=== Deserialized back ===");
            log.info("id: {}", deserialized.getId());
            log.info("hubId: {}", deserialized.getHubId());
            log.info("timestamp: {}", deserialized.getTimestamp());
            if (deserialized.getPayload() != null) {
                log.info("payload: {}", deserialized.getPayload());
            }

        } catch (IOException e) {
            log.error("Error in Avro test utils", e);
        }
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}