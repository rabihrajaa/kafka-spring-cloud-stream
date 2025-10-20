package rabih.kafkaspringcloudstream.handlers;

import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import rabih.kafkaspringcloudstream.events.PageEvent;

import java.util.function.Consumer;
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

}
