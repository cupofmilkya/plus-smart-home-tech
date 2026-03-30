package ru.practicum.controller;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

@RestController
@RequestMapping("/events")
public class CollectorController {

    @PostMapping("/hubs")
    public void collectHub(HubEventAvro hubEventAvro) {

    }

    @PostMapping("/sensors")
    public void collectSensors(SensorEventAvro sensorEventAvro) {

    }
}
