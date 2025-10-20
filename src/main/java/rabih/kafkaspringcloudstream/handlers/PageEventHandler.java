package rabih.kafkaspringcloudstream.handlers;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import rabih.kafkaspringcloudstream.events.PageEvent;

import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

@Component
public class PageEventHandler {
    // Define a Consumer bean to handle incoming PageEvent messages
    @Bean
    public Consumer<PageEvent> pageEventConsumer(){
        return pageEvent -> {
            System.out.println("******************");
            System.out.println(pageEvent.toString());
            System.out.println("******************");
        };
    }

    @Bean
    public Supplier<PageEvent> pageEventSupplier(){
        return ()->new PageEvent(
                Math.random()>0.5?"P1":"P2",
                Math.random()>0.5?"U1":"U2",
                new java.util.Date(),
                10+new java.util.Random().nextInt(10000)
        );
    }
    @Bean
    // Define a Function bean to process KStream of PageEvent messages
    public Function<KStream<String, PageEvent>, KStream<String,Long>> kStreamFunction(){
        return (input)->{
            return input
                    // Filter events with duration greater than 100
                    .filter((k,v)->v.duration()>100)
                    // Map to key-value pairs of page name and duration
                    .map((k,v)->new KeyValue<>(v.nom(), v.duration()))
                    // Group by page name
                    .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                    // Apply time windowing of 5000 seconds
                    .windowedBy(TimeWindows.of(Duration.ofSeconds(5)))
                    // Count occurrences in each window
                    .count(Materialized.as("count-store"))
                    // Convert windowed keys back to string keys
                    .toStream()
                    .map((k,v)->new KeyValue<>(k.key(), v));
        };
    }
}
