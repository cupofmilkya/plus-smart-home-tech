package ru.practicum.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.practicum.entity.hub.HubEvent;
import ru.practicum.entity.sensor.SensorEvent;
import ru.practicum.kafka.KafkaProducerService;
import ru.practicum.mapper.hub.HubEventMapper;
import ru.practicum.mapper.sensor.SensorEventMapper;

@Slf4j
@RestController
@RequestMapping("/events")
@RequiredArgsConstructor
public class CollectorController {

    private final KafkaProducerService kafkaProducerService;

    @PostMapping("/hubs")
    public void collectHub(@RequestBody HubEvent hubEvent) {
        log.info("=== Received hub event ===");
        log.info("Event: {}", hubEvent);

        try {
            log.debug("Starting hub event mapping to Avro...");
            var avroEvent = HubEventMapper.toAvro(hubEvent);
            log.debug("Hub event mapped successfully to Avro");

            log.info("Sending hub event to Kafka...");
            kafkaProducerService.sendHubEvent(avroEvent);

        } catch (Exception e) {
            log.error("Error processing hub event: {}", hubEvent.toString(), e);
            throw e;
        }
    }

    @PostMapping("/sensors")
    public void collectSensor(@RequestBody SensorEvent sensorEvent) {
        log.info("=== Received sensor event ===");
        log.info("Event: {}", sensorEvent);

        try {
            log.debug("Starting sensor event mapping to Avro...");
            var avroEvent = SensorEventMapper.toAvro(sensorEvent);
            log.debug("Sensor event mapped successfully to Avro");

            log.info("Sending sensor event to Kafka...");
            kafkaProducerService.sendSensorEvent(avroEvent);

        } catch (Exception e) {
            log.error("Error processing sensor event: {}", sensorEvent.toString(), e);
            throw e;
        }
    }
}