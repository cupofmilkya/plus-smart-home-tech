package ru.yandex.practicum.serialization;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

@Slf4j
public abstract class BaseAvroSerializer<T extends SpecificRecordBase> implements Serializer<T> {

    private final Schema schema;

    public BaseAvroSerializer(Schema schema) {
        this.schema = schema;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        log.debug("Configuring serializer for schema: {}", schema.getFullName());
    }

    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null) {
            log.warn("Attempting to serialize null data for topic: {}", topic);
            return null;
        }

        try {
            DatumWriter<T> writer = new SpecificDatumWriter<>(schema);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            writer.write(data, encoder);
            encoder.flush();
            return out.toByteArray();
        } catch (IOException e) {
            log.error("Error serializing Avro message for topic: {}", topic, e);
            throw new SerializationException("Error serializing Avro message", e);
        }
    }

    @Override
    public void close() {
        log.debug("Closing serializer");
    }
}