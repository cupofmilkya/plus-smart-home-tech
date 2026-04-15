package ru.yandex.practicum.repository;

import org.springframework.stereotype.Repository;
import ru.yandex.practicum.entity.Action;
import org.springframework.data.jpa.repository.JpaRepository;

@Repository
public interface ActionRepository extends JpaRepository<Action, Long> {
}