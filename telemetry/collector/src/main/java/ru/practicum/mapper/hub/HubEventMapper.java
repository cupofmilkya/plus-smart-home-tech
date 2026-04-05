package ru.practicum.mapper.hub;

import lombok.extern.slf4j.Slf4j;
import ru.practicum.entity.hub.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

@Slf4j
public class HubEventMapper {

    public static HubEventAvro toAvro(HubEvent event) {
        log.debug("Mapping HubEvent to Avro: type={}, hubId={}, timestamp={}",
                event.getType(), event.getHubId(), event.getTimestamp());

        HubEventAvro.Builder builder = HubEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp());

        switch (event.getType()) {

            case HubEventType.DEVICE_ADDED -> {
                DeviceAddedEvent e = (DeviceAddedEvent) event;
                log.debug("Mapping DeviceAddedEvent: id={}, deviceType={}", e.getId(), e.getDeviceType());

                DeviceAddedEventAvro payload = DeviceAddedEventAvro.newBuilder()
                        .setId(e.getId())
                        .setType(mapDeviceType(e.getDeviceType()))
                        .build();

                builder.setPayload(payload);
                log.debug("DeviceAddedEventAvro created: id={}, type={}", payload.getId(), payload.getType());
            }

            case HubEventType.DEVICE_REMOVED -> {
                DeviceRemovedEvent e = (DeviceRemovedEvent) event;
                log.debug("Mapping DeviceRemovedEvent: id={}", e.getId());

                DeviceRemovedEventAvro payload = DeviceRemovedEventAvro.newBuilder()
                        .setId(e.getId())
                        .build();

                builder.setPayload(payload);
                log.debug("DeviceRemovedEventAvro created: id={}", payload.getId());
            }

            case HubEventType.SCENARIO_ADDED -> {
                ScenarioAddedEvent e = (ScenarioAddedEvent) event;
                log.debug("Mapping ScenarioAddedEvent: name={}, conditionsCount={}, actionsCount={}",
                        e.getName(),
                        e.getConditions() != null ? e.getConditions().size() : 0,
                        e.getActions() != null ? e.getActions().size() : 0);

                ScenarioAddedEventAvro payload = ScenarioAddedEventAvro.newBuilder()
                        .setName(e.getName())
                        .setConditions(
                                e.getConditions().stream()
                                        .map(HubEventMapper::mapCondition)
                                        .toList()
                        )
                        .setActions(
                                e.getActions().stream()
                                        .map(HubEventMapper::mapAction)
                                        .toList()
                        )
                        .build();

                builder.setPayload(payload);
                log.debug("ScenarioAddedEventAvro created: name={}, conditions={}, actions={}",
                        payload.getName(),
                        payload.getConditions().size(),
                        payload.getActions().size());
            }

            case HubEventType.SCENARIO_REMOVED -> {
                ScenarioRemovedEvent e = (ScenarioRemovedEvent) event;
                log.debug("Mapping ScenarioRemovedEvent: name={}", e.getName());

                ScenarioRemovedEventAvro payload = ScenarioRemovedEventAvro.newBuilder()
                        .setName(e.getName())
                        .build();

                builder.setPayload(payload);
                log.debug("ScenarioRemovedEventAvro created: name={}", payload.getName());
            }

            default -> {
                log.error("Unknown hub event type: {}", event.getType());
                throw new IllegalArgumentException("Unknown hub event type: " + event.getType());
            }
        }

        HubEventAvro result = builder.build();
        log.info("Successfully mapped HubEvent to Avro: type={}, hubId={}",
                event.getType(), result.getHubId());

        return result;
    }

    private static DeviceTypeAvro mapDeviceType(DeviceType type) {
        log.debug("Mapping DeviceType: {} -> Avro", type);
        return switch (type) {
            case MOTION_SENSOR -> DeviceTypeAvro.MOTION_SENSOR;
            case TEMPERATURE_SENSOR -> DeviceTypeAvro.TEMPERATURE_SENSOR;
            case LIGHT_SENSOR -> DeviceTypeAvro.LIGHT_SENSOR;
            case CLIMATE_SENSOR -> DeviceTypeAvro.CLIMATE_SENSOR;
            case SWITCH_SENSOR -> DeviceTypeAvro.SWITCH_SENSOR;
        };
    }

    private static ScenarioConditionAvro mapCondition(ScenarioCondition c) {
        log.debug("Mapping ScenarioCondition: sensorId={}, type={}, operation={}, value={}",
                c.getSensorId(), c.getType(), c.getOperation(), c.getValue());

        return ScenarioConditionAvro.newBuilder()
                .setSensorId(c.getSensorId())
                .setType(mapConditionType(c.getType()))
                .setOperation(mapOperation(c.getOperation()))
                .setValue(c.getValue())
                .build();
    }

    private static DeviceActionAvro mapAction(DeviceAction a) {
        log.debug("Mapping DeviceAction: sensorId={}, type={}, value={}",
                a.getSensorId(), a.getType(), a.getValue());

        return DeviceActionAvro.newBuilder()
                .setSensorId(a.getSensorId())
                .setType(mapActionType(a.getType()))
                .setValue(a.getValue())
                .build();
    }

    private static ConditionTypeAvro mapConditionType(ConditionType type) {
        log.debug("Mapping ConditionType: {} -> Avro", type);
        return switch (type) {
            case MOTION -> ConditionTypeAvro.MOTION;
            case LUMINOSITY -> ConditionTypeAvro.LUMINOSITY;
            case SWITCH -> ConditionTypeAvro.SWITCH;
            case TEMPERATURE -> ConditionTypeAvro.TEMPERATURE;
            case CO2LEVEL -> ConditionTypeAvro.CO2LEVEL;
            case HUMIDITY -> ConditionTypeAvro.HUMIDITY;
        };
    }

    private static ConditionOperationAvro mapOperation(ConditionOperation op) {
        log.debug("Mapping ConditionOperation: {} -> Avro", op);
        return switch (op) {
            case EQUALS -> ConditionOperationAvro.EQUALS;
            case GREATER_THAN -> ConditionOperationAvro.GREATER_THAN;
            case LOWER_THAN -> ConditionOperationAvro.LOWER_THAN;
        };
    }

    private static ActionTypeAvro mapActionType(ActionType type) {
        log.debug("Mapping ActionType: {} -> Avro", type);
        return switch (type) {
            case ACTIVATE -> ActionTypeAvro.ACTIVATE;
            case DEACTIVATE -> ActionTypeAvro.DEACTIVATE;
            case INVERSE -> ActionTypeAvro.INVERSE;
            case SET_VALUE -> ActionTypeAvro.SET_VALUE;
        };
    }
}