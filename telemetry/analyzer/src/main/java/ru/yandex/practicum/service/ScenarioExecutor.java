package ru.yandex.practicum.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.entity.Snapshot;
import ru.yandex.practicum.entity.Condition;
import ru.yandex.practicum.entity.Scenario;
import ru.yandex.practicum.entity.Action;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Service
@Slf4j
public class ScenarioExecutor {

    public List<DeviceActionProto> evaluateScenario(Scenario scenario, Snapshot snapshot) {
        boolean allConditionsMet = scenario.getConditions().stream()
                .allMatch(condition -> {
                    Object sensorValue = snapshot.getValue(condition.getSensor().getId());
                    return checkCondition(condition, sensorValue);
                });

        if (!allConditionsMet) {
            return Collections.emptyList();
        }

        return scenario.getActions().stream()
                .map(this::convertToDeviceActionProto)
                .collect(Collectors.toList());
    }

    private DeviceActionProto convertToDeviceActionProto(Action action) {
        DeviceActionProto.Builder builder = DeviceActionProto.newBuilder()
                .setType(action.getType())
                .setSensorId(action.getSensorId());

        if (action.getValue() != null) {
            builder.setValue(action.getValue());
        }

        return builder.build();
    }

    private boolean checkCondition(Condition condition, Object sensorValue) {
        if (sensorValue == null) {
            log.debug("Condition check failed: sensor value is null for condition type={}", condition.getType());
            return false;
        }

        boolean result;

        if (sensorValue instanceof Boolean) {
            boolean boolValue = (Boolean) sensorValue;
            int conditionValue = condition.getValue();

            switch (condition.getOperation()) {
                case EQUALS:
                    result = (boolValue && conditionValue == 1) || (!boolValue && conditionValue == 0);
                    break;
                default:
                    log.warn("Unsupported operation {} for Boolean value", condition.getOperation());
                    result = false;
            }

            log.debug("Condition check: type={}, operation={}, sensorValue={}, conditionValue={}, result={}",
                    condition.getType(), condition.getOperation(), boolValue, conditionValue, result);
            return result;
        }

        try {
            int intValue = ((Number) sensorValue).intValue();
            int conditionValue = condition.getValue();

            switch (condition.getOperation()) {
                case EQUALS:
                    result = intValue == conditionValue;
                    break;
                case GREATER_THAN:
                    result = intValue > conditionValue;
                    break;
                case LOWER_THAN:
                    result = intValue < conditionValue;
                    break;
                default:
                    result = false;
            }

            log.debug("Condition check: type={}, operation={}, sensorValue={}, conditionValue={}, result={}",
                    condition.getType(), condition.getOperation(), intValue, conditionValue, result);

            return result;
        } catch (ClassCastException e) {
            log.error("Failed to cast sensor value to Number: value={}, type={}",
                    sensorValue, sensorValue.getClass().getSimpleName(), e);
            return false;
        }
    }
}