package ru.practicum.entity.hub;

public class DeviceAddedEvent extends HubEvent{
    String id;
    private DeviceType type;

    @Override
    public HubEventType getType() {
        return HubEventType.DeviceAddedEvent;
    }
}
