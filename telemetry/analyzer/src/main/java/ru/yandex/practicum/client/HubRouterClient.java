package ru.yandex.practicum.client;

import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionRequest;
import ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerGrpc.HubRouterControllerBlockingStub;
import com.google.protobuf.Timestamp;

@Service
@Slf4j
public class HubRouterClient {

    private final HubRouterControllerBlockingStub hubRouterClient;

    public HubRouterClient(@GrpcClient("hub-router")
                           HubRouterControllerBlockingStub hubRouterClient) {
        this.hubRouterClient = hubRouterClient;
    }

    public void sendAction(String hubId, String scenarioName, String sensorId,
                           DeviceActionProto action, Timestamp timestamp) {
        log.info("Preparing to send action: hubId={}, scenarioName={}, sensorId={}, actionType={}",
                hubId, scenarioName, sensorId, action.getType());

        DeviceActionRequest request = DeviceActionRequest.newBuilder()
                .setHubId(hubId)
                .setScenarioName(scenarioName)
                .setSensorId(sensorId)
                .setAction(action)
                .setTimestamp(timestamp)
                .build();

        try {
            hubRouterClient.handleDeviceAction(request);
            log.info("Action sent successfully to HubRouter for hubId={}, scenarioName={}", hubId, scenarioName);
        } catch (Exception e) {
            log.error("Failed to send action to HubRouter: hubId={}, scenarioName={}", hubId, scenarioName, e);
            throw e;
        }
    }
}