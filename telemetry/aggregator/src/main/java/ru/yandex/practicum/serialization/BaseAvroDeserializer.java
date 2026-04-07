package ru.yandex.practicum.serialization;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;

@Slf4j
public abstract class BaseAvroDeserializer<T extends SpecificRecordBase> implements Deserializer<T> {

    private final DecoderFactory decoderFactory;
    private final Schema schema;

    public BaseAvroDeserializer(Schema schema) {
        this(DecoderFactory.get(), schema);
    }

    public BaseAvroDeserializer(DecoderFactory decoderFactory, Schema schema) {
        this.decoderFactory = decoderFactory;
        this.schema = schema;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        log.debug("Configuring deserializer for schema: {}", schema.getFullName());
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) {
            log.warn("Received null data for topic: {}", topic);
            return null;
        }

        try {
            SpecificDatumReader<T> reader = new SpecificDatumReader<>(schema);
            ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
            Decoder decoder = decoderFactory.binaryDecoder(inputStream, null);
            return reader.read(null, decoder);
        } catch (IOException e) {
            log.error("Error deserializing Avro message from topic: {}", topic, e);
            throw new SerializationException("Error deserializing Avro message", e);
        }
    }

    @Override
    public void close() {
        log.debug("Closing deserializer");
    }
}