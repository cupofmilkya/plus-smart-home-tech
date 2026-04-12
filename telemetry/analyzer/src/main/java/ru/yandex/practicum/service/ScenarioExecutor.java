package ru.yandex.practicum.service;

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
                .setType(action.getType());

        if (action.getValue() != null) {
            builder.setValue(action.getValue());
        }

        return builder.build();
    }

    private boolean checkCondition(Condition condition, Object sensorValue) {
        if (sensorValue == null) {
            return false;
        }

        int intValue = ((Number) sensorValue).intValue();
        int conditionValue = condition.getValue();

        switch (condition.getOperation()) {
            case EQUALS:
                return intValue == conditionValue;
            case GREATER_THAN:
                return intValue > conditionValue;
            case LOWER_THAN:
                return intValue < conditionValue;
            default:
                return false;
        }
    }
}