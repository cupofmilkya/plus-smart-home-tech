package ru.yandex.practicum.entity;

import ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

@Entity
@Table(name = "actions")
@Getter
@Setter
public class Action {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Enumerated(EnumType.STRING)
    private ActionTypeProto type;

    private Integer value;
}