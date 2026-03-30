package ru.practicum.mapper.hub;

import org.springframework.stereotype.Component;
import ru.practicum.entity.hub.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.util.stream.Collectors;

@Component
public class HubEventMapper {

    public static HubEventAvro toAvro(HubEvent event) {
        HubEventAvro avro = new HubEventAvro();

        avro.setHubId(event.getHubId());
        avro.setTimestamp(event.getTimestamp());

        switch (event.getType()) {

            case DeviceAddedEvent -> {
                DeviceAddedEvent e = (DeviceAddedEvent) event;

                DeviceAddedEventAvro payload = new DeviceAddedEventAvro();
                payload.setId(e.getId());
                payload.setType(DeviceTypeAvro.valueOf(e.getType().name()));

                avro.setPayload(payload);
            }

            case DeviceRemovedEvent -> {
                DeviceRemovedEvent e = (DeviceRemovedEvent) event;

                DeviceRemovedEventAvro payload = new DeviceRemovedEventAvro();
                payload.setId(e.getId());

                avro.setPayload(payload);
            }

            case ScenarioAddedEvent -> {
                ScenarioAddedEvent e = (ScenarioAddedEvent) event;

                ScenarioAddedEventAvro payload = new ScenarioAddedEventAvro();
                payload.setName(e.getName());

                payload.setConditions(
                        e.getConditions().stream()
                                .map(HubEventMapper::mapCondition)
                                .collect(Collectors.toList())
                );

                payload.setActions(
                        e.getActions().stream()
                                .map(HubEventMapper::mapAction)
                                .collect(Collectors.toList())
                );

                avro.setPayload(payload);
            }

            case ScenarioRemovedEvent -> {
                ScenarioRemovedEvent e = (ScenarioRemovedEvent) event;

                ScenarioRemovedEventAvro payload = new ScenarioRemovedEventAvro();
                payload.setName(e.getName());

                avro.setPayload(payload);
            }
        }

        return avro;
    }

    private static ScenarioConditionAvro mapCondition(ScenarioCondition c) {
        ScenarioConditionAvro avro = new ScenarioConditionAvro();

        avro.setSensorId(c.getSensorId());
        avro.setType(ConditionTypeAvro.valueOf(c.getType().name()));
        avro.setOperation(ConditionOperationAvro.valueOf(c.getOperation().name()));

        // union {null, int, boolean}
        avro.setValue(c.getValue());

        return avro;
    }

    private static DeviceActionAvro mapAction(DeviceAction a) {
        DeviceActionAvro avro = new DeviceActionAvro();

        avro.setSensorId(a.getSensorId());
        avro.setType(ActionTypeAvro.valueOf(a.getType().name()));
        avro.setValue(a.getValue());

        return avro;
    }
}