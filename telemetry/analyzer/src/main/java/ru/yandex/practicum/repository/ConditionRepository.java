package ru.yandex.practicum.repository;

import org.springframework.stereotype.Repository;
import ru.yandex.practicum.entity.Condition;
import org.springframework.data.jpa.repository.JpaRepository;

@Repository
public interface ConditionRepository extends JpaRepository<Condition, Long> {
}