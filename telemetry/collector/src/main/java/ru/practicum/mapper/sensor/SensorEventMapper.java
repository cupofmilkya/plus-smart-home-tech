package ru.practicum.mapper.sensor;

import org.springframework.stereotype.Component;
import ru.practicum.entity.sensor.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

@Component
public class SensorEventMapper {

    public SensorEventAvro toAvro(SensorEvent event) {
        SensorEventAvro avro = new SensorEventAvro();

        avro.setId(event.getId());
        avro.setHubId(event.getHubId());
        avro.setTimestamp(event.getTimestamp());

        switch (event.getType()) {

            case LIGHT_SENSOR_EVENT -> {
                LightSensorEvent e = (LightSensorEvent) event;

                LightSensorAvro payload = new LightSensorAvro();
                payload.setLinkQuality(e.getLinkQuality());
                payload.setLuminosity(e.getLuminosity());

                avro.setPayload(payload);
            }

            case MOTION_SENSOR_EVENT -> {
                MotionSensorEvent e = (MotionSensorEvent) event;

                MotionSensorAvro payload = new MotionSensorAvro();
                payload.setLinkQuality(e.getLinkQuality());
                payload.setMotion(e.isMotion());
                payload.setVoltage(e.getVoltage());

                avro.setPayload(payload);
            }

            case SWITCH_SENSOR_EVENT -> {
                SwitchSensorEvent e = (SwitchSensorEvent) event;

                SwitchSensorAvro payload = new SwitchSensorAvro();
                payload.setState(e.isState());

                avro.setPayload(payload);
            }

            case CLIMATE_SENSOR_EVENT -> {
                ClimateSensorEvent e = (ClimateSensorEvent) event;

                ClimateSensorAvro payload = new ClimateSensorAvro();
                payload.setTemperatureC(e.getTemperatureC());
                payload.setHumidity(e.getHumidity());
                payload.setCo2Level(e.getCo2Level());

                avro.setPayload(payload);
            }

            case TEMPERATURE_SENSOR_EVENT -> {
                TemperatureSensorEvent e = (TemperatureSensorEvent) event;

                TemperatureSensorAvro payload = new TemperatureSensorAvro();
                payload.setId(e.getId());
                payload.setHubId(e.getHubId());
                payload.setTimestamp(e.getTimestamp());
                payload.setTemperatureC(e.getTemperatureC());
                payload.setTemperatureF(e.getTemperatureF());

                avro.setPayload(payload);
            }
        }

        return avro;
    }
}