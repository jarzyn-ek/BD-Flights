package anomalies;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class MyEventTimeExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp) {

        long timestamp = -1;
        String stringLine;

        if (record.value() instanceof String) {
            stringLine = (String) record.value();
            timestamp = FlightsRecord.parseFromLogLine(
                    stringLine
            ).getTimestampInMillis();
        }

        return timestamp;
    }
}
