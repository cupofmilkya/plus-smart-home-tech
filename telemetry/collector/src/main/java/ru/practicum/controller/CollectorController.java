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
        log.info("Received hub event: {}", hubEvent);
        kafkaProducerService.sendHubEvent(
                HubEventMapper.toAvro(hubEvent)
        );
    }

    @PostMapping("/sensors")
    public void collectSensor(@RequestBody SensorEvent sensorEvent) {
        log.info("Received sensor event: {}", sensorEvent);
        kafkaProducerService.sendSensorEvent(
                SensorEventMapper.toAvro(sensorEvent)
        );
    }
}