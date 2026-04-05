package ru.practicum.mapper.sensor;

import lombok.extern.slf4j.Slf4j;
import ru.practicum.entity.sensor.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

@Slf4j
public class SensorEventMapper {

    public static SensorEventAvro toAvro(SensorEvent event) {
        log.debug("Mapping SensorEvent to Avro: type={}, id={}, hubId={}, timestamp={}",
                event.getType(), event.getId(), event.getHubId(), event.getTimestamp());

        SensorEventAvro.Builder builder = SensorEventAvro.newBuilder()
                .setId(event.getId())
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp());

        switch (event.getType()) {

            case SensorEventType.CLIMATE_SENSOR_EVENT -> {
                ClimateSensorEvent e = (ClimateSensorEvent) event;
                log.debug("Mapping ClimateSensorEvent: temperatureC={}, humidity={}, co2Level={}",
                        e.getTemperatureC(), e.getHumidity(), e.getCo2Level());

                ClimateSensorAvro climatePayload = ClimateSensorAvro.newBuilder()
                        .setTemperatureC(e.getTemperatureC())
                        .setHumidity(e.getHumidity())
                        .setCo2Level(e.getCo2Level())
                        .build();

                builder.setPayload(climatePayload);
                log.debug("ClimateSensorAvro created successfully");
            }

            case SensorEventType.LIGHT_SENSOR_EVENT -> {
                LightSensorEvent e = (LightSensorEvent) event;
                log.debug("Mapping LightSensorEvent: linkQuality={}, luminosity={}",
                        e.getLinkQuality(), e.getLuminosity());

                LightSensorAvro lightPayload = LightSensorAvro.newBuilder()
                        .setLinkQuality(e.getLinkQuality())
                        .setLuminosity(e.getLuminosity())
                        .build();

                builder.setPayload(lightPayload);
                log.debug("LightSensorAvro created successfully");
            }

            case SensorEventType.MOTION_SENSOR_EVENT -> {
                MotionSensorEvent e = (MotionSensorEvent) event;
                log.debug("Mapping MotionSensorEvent: linkQuality={}, motion={}, voltage={}",
                        e.getLinkQuality(), e.isMotion(), e.getVoltage());

                MotionSensorAvro motionPayload = MotionSensorAvro.newBuilder()
                        .setLinkQuality(e.getLinkQuality())
                        .setMotion(e.isMotion())
                        .setVoltage(e.getVoltage())
                        .build();

                builder.setPayload(motionPayload);
                log.debug("MotionSensorAvro created successfully");
            }

            case SensorEventType.SWITCH_SENSOR_EVENT -> {
                SwitchSensorEvent e = (SwitchSensorEvent) event;
                log.debug("Mapping SwitchSensorEvent: state={}", e.isState());

                SwitchSensorAvro switchPayload = SwitchSensorAvro.newBuilder()
                        .setState(e.isState())
                        .build();

                builder.setPayload(switchPayload);
                log.debug("SwitchSensorAvro created successfully");
            }

            case SensorEventType.TEMPERATURE_SENSOR_EVENT -> {
                TemperatureSensorEvent e = (TemperatureSensorEvent) event;
                log.debug("Mapping TemperatureSensorEvent: temperatureC={}, temperatureF={}",
                        e.getTemperatureC(), e.getTemperatureF());

                TemperatureSensorAvro temperaturePayload = TemperatureSensorAvro.newBuilder()
                        .setId(e.getId())
                        .setHubId(e.getHubId())
                        .setTimestamp(e.getTimestamp())
                        .setTemperatureC(e.getTemperatureC())
                        .setTemperatureF(e.getTemperatureF())
                        .build();

                builder.setPayload(temperaturePayload);
                log.debug("TemperatureSensorAvro created successfully with all fields: id={}, hubId={}, timestamp={}",
                        temperaturePayload.getId(), temperaturePayload.getHubId(), temperaturePayload.getTimestamp());
            }

            default -> {
                log.error("Unknown sensor event type: {}", event.getType());
                throw new IllegalArgumentException("Unknown sensor event type: " + event.getType());
            }
        }

        SensorEventAvro result = builder.build();
        log.info("Successfully mapped SensorEvent to Avro: type={}, id={}, hubId={}",
                event.getType(), result.getId(), result.getHubId());

        if (result.getPayload() != null) {
            log.debug("Payload details: {}", result.getPayload());

            // Если это ClimateSensorAvro
            if (result.getPayload() instanceof ClimateSensorAvro climate) {
                log.info("Climate payload: temperatureC={}, humidity={}, co2Level={}",
                        climate.getTemperatureC(), climate.getHumidity(), climate.getCo2Level());
            }
            // Если это LightSensorAvro
            else if (result.getPayload() instanceof LightSensorAvro light) {
                log.info("Light payload: linkQuality={}, luminosity={}",
                        light.getLinkQuality(), light.getLuminosity());
            }
            // Если это MotionSensorAvro
            else if (result.getPayload() instanceof MotionSensorAvro motion) {
                log.info("Motion payload: linkQuality={}, motion={}, voltage={}",
                        motion.getLinkQuality(), motion.getMotion(), motion.getVoltage());
            }
            // Если это SwitchSensorAvro
            else if (result.getPayload() instanceof SwitchSensorAvro switchSensor) {
                log.info("Switch payload: state={}", switchSensor.getState());
            }
            // Если это TemperatureSensorAvro
            else if (result.getPayload() instanceof TemperatureSensorAvro temp) {
                log.info("Temperature payload: id={}, hubId={}, timestamp={}, tempC={}, tempF={}",
                        temp.getId(), temp.getHubId(), temp.getTimestamp(),
                        temp.getTemperatureC(), temp.getTemperatureF());
            }
        }

        return result;
    }
}