package ru.yandex.practicum.consumers;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Value;
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
                records.forEach(record -> {
                    try {
                        HubEventProto event = HubEventProto.parseFrom(record.value());
                        processHubEvent(event);
                    } catch (Exception e) {
                        log.error("Error processing hub event", e);
                    }
                });
            }
        } catch (Exception e) {
            log.error("Fatal error in HubEventProcessor", e);
        }
    }

    private void processHubEvent(HubEventProto event) {
        String hubId = event.getHubId();

        if (event.hasDeviceAddedEvent()) {
            handleDeviceAdded(hubId, event.getDeviceAddedEvent());
        } else if (event.hasDeviceRemovedEvent()) {
            handleDeviceRemoved(hubId, event.getDeviceRemovedEvent());
        } else if (event.hasScenarioAddedEvent()) {
            handleScenarioAdded(hubId, event.getScenarioAddedEvent());
        } else if (event.hasScenarioRemovedEvent()) {
            handleScenarioRemoved(hubId, event.getScenarioRemovedEvent());
        } else {
            log.warn("Unknown hub event type for hubId: {}", hubId);
        }
    }

    private void handleDeviceAdded(String hubId, DeviceAddedEventProto device) {
        String sensorId = device.getId();

        // Проверяем, существует ли уже датчик
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

    private void handleDeviceRemoved(String hubId, DeviceRemovedEventProto device) {
        String sensorId = device.getId();

        // Удаляем датчик
        sensorRepository.deleteById(sensorId);
        log.info("Device removed: id={}, hubId={}", sensorId, hubId);
    }

    private void handleScenarioAdded(String hubId, ScenarioAddedEventProto scenarioProto) {
        String scenarioName = scenarioProto.getName();

        // Проверяем, существует ли уже сценарий с таким именем
        scenarioRepository.findByHubIdAndName(hubId, scenarioName)
                .ifPresent(existingScenario -> {
                    log.info("Scenario already exists, updating: hubId={}, name={}", hubId, scenarioName);
                    // Удаляем старый сценарий и его связи
                    scenarioRepository.delete(existingScenario);
                });

        // Находим все сенсоры, необходимые для условий
        Map<String, Sensor> sensors = new HashMap<>();
        for (ScenarioConditionProto conditionProto : scenarioProto.getConditionList()) {
            String sensorId = conditionProto.getSensorId();
            sensors.computeIfAbsent(sensorId, id -> sensorRepository.findById(id)
                    .orElseThrow(() -> new RuntimeException("Sensor not found: " + id)));
        }

        // Создаём новый сценарий
        Scenario scenario = new Scenario();
        scenario.setHubId(hubId);
        scenario.setName(scenarioName);

        // Конвертируем и сохраняем условия (с привязкой к сенсорам)
        List<Condition> conditions = scenarioProto.getConditionList().stream()
                .map(cp -> convertToCondition(cp, sensors.get(cp.getSensorId())))
                .collect(Collectors.toList());
        conditionRepository.saveAll(conditions);
        scenario.setConditions(conditions);

        // Конвертируем и сохраняем действия
        List<Action> actions = scenarioProto.getActionList().stream()
                .map(this::convertToAction)
                .collect(Collectors.toList());
        actionRepository.saveAll(actions);
        scenario.setActions(actions);

        // Сохраняем сценарий
        scenarioRepository.save(scenario);

        log.info("Scenario added: hubId={}, name={}, conditions={}, actions={}",
                hubId, scenarioName, conditions.size(), actions.size());
    }

    private void handleScenarioRemoved(String hubId, ScenarioRemovedEventProto scenarioProto) {
        String scenarioName = scenarioProto.getName();

        scenarioRepository.findByHubIdAndName(hubId, scenarioName)
                .ifPresent(scenario -> {
                    scenarioRepository.delete(scenario);
                    log.info("Scenario removed: hubId={}, name={}", hubId, scenarioName);
                });

        if (scenarioRepository.findByHubIdAndName(hubId, scenarioName).isEmpty()) {
            log.warn("Scenario not found for removal: hubId={}, name={}", hubId, scenarioName);
        }
    }

    private Condition convertToCondition(ScenarioConditionProto conditionProto) {
        Condition condition = new Condition();
        condition.setType(conditionProto.getType());
        condition.setOperation(conditionProto.getOperation());

        // Устанавливаем значение в зависимости от типа
        if (conditionProto.hasIntValue()) {
            condition.setValue(conditionProto.getIntValue());
        } else if (conditionProto.hasBoolValue()) {
            condition.setValue(conditionProto.getBoolValue() ? 1 : 0);
        }

        return condition;
    }

    private Condition convertToCondition(ScenarioConditionProto conditionProto, Sensor sensor) {
        Condition condition = new Condition();
        condition.setType(conditionProto.getType());
        condition.setOperation(conditionProto.getOperation());
        condition.setSensor(sensor);

        // Устанавливаем значение в зависимости от типа
        if (conditionProto.hasIntValue()) {
            condition.setValue(conditionProto.getIntValue());
        } else if (conditionProto.hasBoolValue()) {
            condition.setValue(conditionProto.getBoolValue() ? 1 : 0);
        }

        return condition;
    }

    private Action convertToAction(DeviceActionProto actionProto) {
        Action action = new Action();
        action.setType(actionProto.getType());

        if (actionProto.hasValue()) {
            action.setValue(actionProto.getValue());
        }

        return action;
    }

    public void stop() {
        running = false;
        log.info("HubEventProcessor stopped");
    }
}