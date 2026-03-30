package ru.practicum.controller;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.practicum.entity.hub.HubEvent;
import ru.practicum.entity.sensor.SensorEvent;

@RestController
@RequestMapping("/events")
public class CollectorController {

    @PostMapping("/hubs")
    public void collectHub(HubEvent hubEvent) {

    }

    @PostMapping("/sensors")
    public void collectSensor(SensorEvent sensorEvent) {

    }
}
