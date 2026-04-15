package ru.practicum.controller;

import io.grpc.stub.StreamObserver;
import com.google.protobuf.Empty;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;
import ru.practicum.kafka.KafkaProducerService;
import ru.practicum.mapper.hub.HubEventGrpcMapper;
import ru.practicum.mapper.sensor.SensorEventGrpcMapper;
import ru.yandex.practicum.grpc.telemetry.collector.CollectorControllerGrpc;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;

@Slf4j
@GrpcService
@RequiredArgsConstructor
public class CollectorGrpcController extends CollectorControllerGrpc.CollectorControllerImplBase {

    private final KafkaProducerService kafkaProducerService;

    @Override
    public void collectSensorEvent(SensorEventProto request, StreamObserver<Empty> responseObserver) {
        log.info("=== Received gRPC sensor event ===");
        log.info("Event id: {}", request.getId());
        log.info("Hub id: {}", request.getHubId());
        log.info("Timestamp: {}", request.getTimestamp());

        try {
            var avroEvent = SensorEventGrpcMapper.toAvro(request);
            kafkaProducerService.sendSensorEvent(avroEvent);

            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
            log.info("Sensor event processed successfully");

        } catch (Exception e) {
            log.error("Error processing gRPC sensor event: {}", request.getId(), e);
            responseObserver.onError(new io.grpc.StatusRuntimeException(
                    io.grpc.Status.INTERNAL
                            .withDescription(e.getLocalizedMessage())
                            .withCause(e)
            ));
        }
    }

    @Override
    public void collectHubEvent(HubEventProto request, StreamObserver<Empty> responseObserver) {
        log.info("=== Received gRPC hub event ===");
        log.info("Hub id: {}", request.getHubId());
        log.info("Timestamp: {}", request.getTimestamp());

        if (request.hasDeviceAddedEvent()) {
            log.info("Device added: {}", request.getDeviceAddedEvent().getId());
        } else if (request.hasDeviceRemovedEvent()) {
            log.info("Device removed: {}", request.getDeviceRemovedEvent().getId());
        } else if (request.hasScenarioAddedEvent()) {
            log.info("Scenario added: {}", request.getScenarioAddedEvent().getName());
        } else if (request.hasScenarioRemovedEvent()) {
            log.info("Scenario removed: {}", request.getScenarioRemovedEvent().getName());
        }

        try {
            var avroEvent = HubEventGrpcMapper.toAvro(request);
            kafkaProducerService.sendHubEvent(avroEvent);

            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
            log.info("Hub event processed successfully");

        } catch (Exception e) {
            log.error("Error processing gRPC hub event: {}", request.getHubId(), e);
            responseObserver.onError(new io.grpc.StatusRuntimeException(
                    io.grpc.Status.INTERNAL
                            .withDescription(e.getLocalizedMessage())
                            .withCause(e)
            ));
        }
    }
}