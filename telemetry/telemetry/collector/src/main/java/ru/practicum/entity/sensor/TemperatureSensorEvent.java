package ru.practicum.entity.sensor;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class TemperatureSensorEvent extends SensorEvent {
    @JsonProperty("temperature_c")
    private int temperatureC;

    @JsonProperty("temperature_f")
    private int temperatureF;

    @Override
    public SensorEventType getType() {
        return SensorEventType.TEMPERATURE_SENSOR_EVENT;
    }
}