package ru.practicum.entity;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.time.Instant;

@Getter
@Setter
@ToString
public abstract class HubEvent {
    private String id;
    private String hubId;
    private Instant timestamp = Instant.now();

    // абстрактный метод, который должен быть определён в конкретных реализациях
    public abstract HubEventType getType();
}
