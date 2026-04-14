package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.client.HubRouterClient;
import ru.yandex.practicum.entity.Scenario;
import ru.yandex.practicum.entity.Snapshot;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.repository.ScenarioRepository;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class SnapshotProcessingService {

    private final ScenarioRepository scenarioRepository;
    private final ScenarioExecutor scenarioExecutor;
    private final HubRouterClient hubRouterClient;

    @Transactional(readOnly = true)
    public void processSnapshot(Snapshot snapshot) {
        List<Scenario> scenarios = scenarioRepository.findByHubId(snapshot.getHubId());
        log.info("Found {} scenarios for hubId: {}", scenarios.size(), snapshot.getHubId());

        for (Scenario scenario : scenarios) {
            // Принудительно инициализируем ленивые коллекции
            scenario.getConditions().size();
            scenario.getActions().size();

            log.debug("Evaluating scenario: {} for hubId: {}", scenario.getName(), snapshot.getHubId());

            List<DeviceActionProto> actions = scenarioExecutor.evaluateScenario(scenario, snapshot);
            if (!actions.isEmpty()) {
                log.info("Scenario '{}' triggered for hubId: {}, executing {} actions",
                        scenario.getName(), snapshot.getHubId(), actions.size());

                for (DeviceActionProto action : actions) {
                    log.info("Sending action to HubRouter: hubId={}, scenario={}, sensorId={}, type={}, value={}",
                            snapshot.getHubId(), scenario.getName(), action.getSensorId(),
                            action.getType(), action.hasValue() ? action.getValue() : "no value");

                    hubRouterClient.sendAction(
                            snapshot.getHubId(),
                            scenario.getName(),
                            action.getSensorId(),
                            action,
                            com.google.protobuf.Timestamp.newBuilder()
                                    .setSeconds(System.currentTimeMillis() / 1000)
                                    .build()
                    );
                }
            } else {
                log.debug("Scenario '{}' conditions not met for hubId: {}", scenario.getName(), snapshot.getHubId());
            }
        }
    }
}