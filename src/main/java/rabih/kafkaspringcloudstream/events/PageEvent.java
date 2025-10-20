package rabih.kafkaspringcloudstream.events;

import java.util.Date;

public record PageEvent(String nom, String user, Date date, long duration ) {
}
