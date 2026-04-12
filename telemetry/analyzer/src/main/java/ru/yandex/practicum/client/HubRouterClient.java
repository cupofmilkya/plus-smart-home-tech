package ru.yandex.practicum.client;

import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionRequest;
import ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerGrpc.HubRouterControllerBlockingStub;
import com.google.protobuf.Timestamp;

@Service
public class HubRouterClient {

    private final HubRouterControllerBlockingStub hubRouterClient;

    public HubRouterClient(@GrpcClient("hub-router")
                           HubRouterControllerBlockingStub hubRouterClient) {
        this.hubRouterClient = hubRouterClient;
    }

    public void sendAction(String hubId, String scenarioName, String sensorId,
                           DeviceActionProto action, Timestamp timestamp) {
        DeviceActionRequest request = DeviceActionRequest.newBuilder()
                .setHubId(hubId)
                .setScenarioName(scenarioName)
                .setSensorId(sensorId)
                .setAction(action)
                .setTimestamp(timestamp)
                .build();

        hubRouterClient.handleDeviceAction(request);
    }
}