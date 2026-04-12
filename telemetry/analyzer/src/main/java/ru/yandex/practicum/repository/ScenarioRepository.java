package ru.yandex.practicum.repository;

import ru.yandex.practicum.entity.Scenario;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;
import java.util.Optional;

public interface ScenarioRepository extends JpaRepository<Scenario, Long> {
    List<Scenario> findByHubId(String hubId);
    Optional<Scenario> findByHubIdAndName(String hubId, String name);
}