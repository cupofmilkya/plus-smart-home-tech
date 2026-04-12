package ru.yandex.practicum;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.ConfigurableApplicationContext;
import ru.yandex.practicum.consumers.HubEventProcessor;
import ru.yandex.practicum.consumers.SnapshotProcessor;

@SpringBootApplication
@ConfigurationPropertiesScan
public class Analyzer {
    public static void main(String[] args) {
        ConfigurableApplicationContext context =
                SpringApplication.run(Analyzer.class, args);

        HubEventProcessor hubEventProcessor =
                context.getBean(HubEventProcessor.class);
        SnapshotProcessor snapshotProcessor =
                context.getBean(SnapshotProcessor.class);

        // Запускаем в отдельном потоке обработчик событий хабов
        Thread hubEventsThread = new Thread(hubEventProcessor);
        hubEventsThread.setName("HubEventHandlerThread");
        hubEventsThread.start();

        // В текущем потоке начинаем обработку снапшотов
        snapshotProcessor.start();
    }
}