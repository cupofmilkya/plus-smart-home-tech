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
import ru.yandex.practicum.kafka.telemetry.event.*;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.entity.*;
import ru.yandex.practicum.grpc.telemetry.event.*;
import ru.yandex.practicum.repository.*;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class HubEventProcessor implements Runnable {

    private final SensorRepository sensorRepository;
    private final ScenarioRepository scenarioRepository;
    private final ConditionRepository conditionRepository;
    private final ActionRepository actionRepository;

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    private volatile boolean running = true;

    @Override
    public void run() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", "analyzer-hub-group");
        props.put("enable.auto.commit", "true");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of("telemetry.hubs.v1"));
            log.info("HubEventProcessor started, subscribed to telemetry.hubs.v1");

            while (running) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, byte[]> record : records) {
                    try {
                        log.debug("Received message from topic: {}, partition: {}, offset: {}, key: {}, value size: {} bytes",
                                record.topic(), record.partition(), record.offset(), record.key(),
                                record.value() != null ? record.value().length : 0);

                        if (record.value() == null || record.value().length == 0) {
                            log.warn("Empty or null message received");
                            continue;
                        }

                        HubEventAvro event = deserializeHubEvent(record.value());
                        log.info("Successfully parsed HubEventAvro: hubId={}, payloadType={}",
                                event.getHubId(),
                                event.getPayload().getClass().getSimpleName());
                        processHubEvent(event);
                    } catch (Exception e) {
                        log.error("Error processing hub event at offset: {}", record.offset(), e);
                    }
                }
            }
        } catch (Exception e) {
            log.error("Fatal error in HubEventProcessor", e);
        }
    }

    private HubEventAvro deserializeHubEvent(byte[] bytes) {
        try {
            SpecificDatumReader<HubEventAvro> reader = new SpecificDatumReader<>(HubEventAvro.getClassSchema());
            Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
            return reader.read(null, decoder);
        } catch (Exception e) {
            log.error("Failed to deserialize Avro hub event", e);
            throw new RuntimeException("Failed to deserialize hub event", e);
        }
    }

    private void processHubEvent(HubEventAvro event) {
        String hubId = event.getHubId().toString();
        Object payload = event.getPayload();

        log.debug("Processing hub event: hubId={}, payloadType={}", hubId, payload.getClass().getSimpleName());

        if (payload instanceof DeviceAddedEventAvro) {
            handleDeviceAdded(hubId, (DeviceAddedEventAvro) payload);
        } else if (payload instanceof DeviceRemovedEventAvro) {
            handleDeviceRemoved(hubId, (DeviceRemovedEventAvro) payload);
        } else if (payload instanceof ScenarioAddedEventAvro) {
            handleScenarioAdded(hubId, (ScenarioAddedEventAvro) payload);
        } else if (payload instanceof ScenarioRemovedEventAvro) {
            handleScenarioRemoved(hubId, (ScenarioRemovedEventAvro) payload);
        } else {
            log.warn("Unknown hub event payload type: {}", payload.getClass());
        }
    }

    private void handleDeviceAdded(String hubId, DeviceAddedEventAvro device) {
        String sensorId = device.getId().toString();

        if (sensorRepository.existsById(sensorId)) {
            log.info("Device already exists, updating: id={}, hubId={}", sensorId, hubId);
            Sensor existingSensor = sensorRepository.findById(sensorId).get();
            existingSensor.setHubId(hubId);
            sensorRepository.save(existingSensor);
        } else {
            Sensor sensor = new Sensor();
            sensor.setId(sensorId);
            sensor.setHubId(hubId);
            sensorRepository.save(sensor);
        }

        log.info("Device added/updated: id={}, hubId={}, type={}",
                sensorId, hubId, device.getType());
    }

    private void handleDeviceRemoved(String hubId, DeviceRemovedEventAvro device) {
        String sensorId = device.getId().toString();
        sensorRepository.deleteById(sensorId);
        log.info("Device removed: id={}, hubId={}", sensorId, hubId);
    }

    private void handleScenarioAdded(String hubId, ScenarioAddedEventAvro scenarioProto) {
        String scenarioName = scenarioProto.getName().toString();

        scenarioRepository.findByHubIdAndName(hubId, scenarioName)
                .ifPresent(existingScenario -> {
                    log.info("Scenario already exists, updating: hubId={}, name={}", hubId, scenarioName);
                    scenarioRepository.delete(existingScenario);
                });

        // Находим все сенсоры, необходимые для условий
        Map<String, Sensor> sensors = new HashMap<>();
        for (ScenarioConditionAvro conditionProto : scenarioProto.getConditions()) {
            String sensorId = conditionProto.getSensorId().toString();
            sensors.computeIfAbsent(sensorId, id -> sensorRepository.findById(id)
                    .orElseThrow(() -> new RuntimeException("Sensor not found: " + id)));
        }

        // Создаём новый сценарий
        Scenario scenario = new Scenario();
        scenario.setHubId(hubId);
        scenario.setName(scenarioName);

        // Конвертируем и сохраняем условия
        List<Condition> conditions = scenarioProto.getConditions().stream()
                .map(cp -> convertToCondition(cp, sensors.get(cp.getSensorId().toString())))
                .collect(Collectors.toList());
        conditionRepository.saveAll(conditions);
        scenario.setConditions(conditions);

        // Конвертируем и сохраняем действия
        List<Action> actions = scenarioProto.getActions().stream()
                .map(this::convertToAction)
                .collect(Collectors.toList());
        actionRepository.saveAll(actions);
        scenario.setActions(actions);

        scenarioRepository.save(scenario);

        log.info("Scenario added: hubId={}, name={}, conditions={}, actions={}",
                hubId, scenarioName, conditions.size(), actions.size());
    }

    private void handleScenarioRemoved(String hubId, ScenarioRemovedEventAvro scenarioProto) {
        String scenarioName = scenarioProto.getName().toString();

        scenarioRepository.findByHubIdAndName(hubId, scenarioName)
                .ifPresent(scenario -> {
                    scenarioRepository.delete(scenario);
                    log.info("Scenario removed: hubId={}, name={}", hubId, scenarioName);
                });
    }

    private Condition convertToCondition(ScenarioConditionAvro conditionProto, Sensor sensor) {
        Condition condition = new Condition();
        condition.setType(mapConditionType(conditionProto.getType()));
        condition.setOperation(mapOperation(conditionProto.getOperation()));
        condition.setSensor(sensor);

        Object value = conditionProto.getValue();
        if (value instanceof Integer) {
            condition.setValue((Integer) value);
        } else if (value instanceof Boolean) {
            condition.setValue((Boolean) value ? 1 : 0);
        }

        return condition;
    }

    private Action convertToAction(DeviceActionAvro actionProto) {
        Action action = new Action();
        action.setType(mapActionType(actionProto.getType()));
        action.setSensorId(actionProto.getSensorId().toString());

        if (actionProto.getValue() != null) {
            action.setValue(actionProto.getValue());
        }

        return action;
    }

    private ConditionTypeProto mapConditionType(ConditionTypeAvro type) {
        return switch (type) {
            case MOTION -> ConditionTypeProto.MOTION;
            case LUMINOSITY -> ConditionTypeProto.LUMINOSITY;
            case SWITCH -> ConditionTypeProto.SWITCH;
            case TEMPERATURE -> ConditionTypeProto.TEMPERATURE;
            case CO2LEVEL -> ConditionTypeProto.CO2LEVEL;
            case HUMIDITY -> ConditionTypeProto.HUMIDITY;
        };
    }

    private ConditionOperationProto mapOperation(ConditionOperationAvro op) {
        return switch (op) {
            case EQUALS -> ConditionOperationProto.EQUALS;
            case GREATER_THAN -> ConditionOperationProto.GREATER_THAN;
            case LOWER_THAN -> ConditionOperationProto.LOWER_THAN;
        };
    }

    private ActionTypeProto mapActionType(ActionTypeAvro type) {
        return switch (type) {
            case ACTIVATE -> ActionTypeProto.ACTIVATE;
            case DEACTIVATE -> ActionTypeProto.DEACTIVATE;
            case INVERSE -> ActionTypeProto.INVERSE;
            case SET_VALUE -> ActionTypeProto.SET_VALUE;
        };
    }

    public void stop() {
        running = false;
        log.info("HubEventProcessor stopped");
    }
}