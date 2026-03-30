package ru.practicum.entity.hub;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.time.Instant;

@Getter
@Setter
@ToString
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.EXISTING_PROPERTY,
        property = "type"
)
@JsonSubTypes({
        @JsonSubTypes.Type(value = DeviceAddedEvent.class, name = "DeviceAddedEvent"),
        @JsonSubTypes.Type(value = DeviceRemovedEvent.class, name = "DeviceRemovedEvent"),
        @JsonSubTypes.Type(value = ScenarioAddedEvent.class, name = "ScenarioAddedEvent"),
        @JsonSubTypes.Type(value = ScenarioRemovedEvent.class, name = "ScenarioRemovedEvent")
})
public abstract class HubEvent {
    private String id;
    private String hubId;
    private Instant timestamp = Instant.now();

    // абстрактный метод, который должен быть определён в конкретных реализациях
    public abstract HubEventType getType();
}
