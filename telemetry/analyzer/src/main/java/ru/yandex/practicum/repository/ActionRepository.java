package ru.yandex.practicum.repository;

import ru.yandex.practicum.entity.Action;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ActionRepository extends JpaRepository<Action, Long> {
}