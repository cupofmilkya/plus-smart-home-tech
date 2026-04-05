package ru.practicum.entity.hub;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ScenarioCondition {
    private String sensorId;
    private ConditionType type;
    private ConditionOperation operation;

    // может быть int или boolean
    private Object value;
}