package ru.yandex.practicum.service;

import org.springframework.stereotype.Service;
import ru.yandex.practicum.entity.Snapshot;
import ru.yandex.practicum.entity.Condition;
import ru.yandex.practicum.entity.Scenario;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class ScenarioExecutor {

    public List<DeviceActionProto> evaluateScenario(Scenario scenario, Snapshot snapshot) {
        boolean allConditionsMet = scenario.getConditions().stream()
                .allMatch(condition -> checkCondition(condition, snapshot));

        if (allConditionsMet) {
            return scenario.getActions().stream()
                    .map(action -> DeviceActionProto.newBuilder()
                            .setSensorId(scenario.getSensor().getId())
                            .setType(action.getType())
                            .setValue(action.getValue() != null ? action.getValue() : 0)
                            .build())
                    .collect(Collectors.toList());
        }
        return Collections.emptyList();
    }

    private boolean checkCondition(Condition condition, Snapshot snapshot) {
        Object sensorValue = snapshot.getValue(condition.getSensor().getId());

        if (sensorValue == null) {
            return false;
        }

        switch (condition.getOperation()) {
            case EQUALS:
                return sensorValue.equals(condition.getValue());
            case GREATER_THAN:
                return (Integer) sensorValue > condition.getValue();
            case LOWER_THAN:
                return (Integer) sensorValue < condition.getValue();
            default:
                return false;
        }
    }
}