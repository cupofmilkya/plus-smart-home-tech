package ru.yandex.practicum.entity;

import lombok.Builder;
import lombok.Data;
import java.util.Map;

@Data
@Builder
public class Snapshot {
    private String hubId;
    private Map<String, Object> sensorValues;

    public Object getValue(String sensorId) {
        return sensorValues.get(sensorId);
    }
}