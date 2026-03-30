package ru.practicum.entity.sensor;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ClimateSensorEvent extends SensorEvent {
    @JsonProperty("temperature_c")
    private int temperatureC;
    private int humidity;
    @JsonProperty("co2_level")
    private int co2Level;

    @Override
    public SensorEventType getType() {
        return SensorEventType.CLIMATE_SENSOR_EVENT;
    }
}