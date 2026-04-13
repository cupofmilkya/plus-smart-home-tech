package ru.yandex.practicum;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import ru.yandex.practicum.consumers.HubEventProcessor;
import ru.yandex.practicum.consumers.SnapshotProcessor;

@Slf4j
@SpringBootApplication
@ConfigurationPropertiesScan
@EnableJpaRepositories(basePackages = "ru.yandex.practicum.repository")
@EnableTransactionManagement
public class Analyzer {
    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(Analyzer.class, args);

        HubEventProcessor hubEventProcessor = context.getBean(HubEventProcessor.class);
        SnapshotProcessor snapshotProcessor = context.getBean(SnapshotProcessor.class);

        Thread hubEventsThread = new Thread(hubEventProcessor);
        hubEventsThread.setName("HubEventHandlerThread");
        hubEventsThread.start();

        snapshotProcessor.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down Analyzer...");
            snapshotProcessor.stop();
            hubEventProcessor.stop();
            try {
                hubEventsThread.join(5000);
            } catch (InterruptedException e) {
                log.error("Interrupted while waiting for threads to stop", e);
            }
            context.close();
        }));
    }
}