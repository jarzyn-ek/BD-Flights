package anomalies;

import ETL.Airport;
import ETL.FlightsDataTransformer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.io.FileNotFoundException;
import java.sql.SQLException;
import java.text.ParseException;
import java.time.Duration;
import java.util.Properties;

public class FlightDataToAlertRequests {

    public static final int TIME_INTERVAL_IN_MINUTES = 60;
    public static final int MINUTES_BEFORE = 30;
    public static final Long NUMBER_OF_AIRPLANES = new Long(1);

    public static void main(String[] args) throws FileNotFoundException, SQLException {

        FlightsDataTransformer flightsDataTransformer = new FlightsDataTransformer();
        flightsDataTransformer.readAirports();
        flightsDataTransformer.start();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MyEventTimeExtractor.class);

        final Serde<String> stringSerde = Serdes.String();
        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> textLines = builder.stream("quickstart-events", Consumed.with(stringSerde, stringSerde));

        textLines.foreach((k,v) -> {
            try {
                flightsDataTransformer.apply(k,v);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        });


        KStream<String, FlightsRecord> flightsStream = textLines
                .filter((key, value) -> FlightsRecord.isOK(value))
                .mapValues(value -> FlightsRecord.parseFromLogLine(value))
                .filter((key, value) -> value.getInfoType().equals("A"));

        KTable<Windowed<String>, Long> flightsInExactInterval = flightsStream
                .filter((key, value) -> (value.isArrivalTimeInInterval(TIME_INTERVAL_IN_MINUTES, MINUTES_BEFORE)))
                .map((key, value) -> KeyValue.pair(value.getDestAirport(), " "))
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofMinutes(TIME_INTERVAL_IN_MINUTES)))
                .count()
                .filter((k, v) -> v > NUMBER_OF_AIRPLANES);

        KStream<String, String> test = flightsInExactInterval.toStream().map((k,v) -> KeyValue.pair(k.key(),new String(k.key() + " " + v + " " + k.window().startTime().toString() + " " + k.window().endTime())));


        KTable<String, Long> allActualFlights = flightsStream
                .filter((key, value) -> value.isArrivalInFuture())
                .map((key, value) -> KeyValue.pair(value.getDestAirport(), " "))
                .groupByKey()
                .count();

        KStream<String, String> joined = test.join(allActualFlights, (left, right) -> (left + " " + right));

        joined.map((k,v) -> KeyValue.pair(k, createOutputAnomaliesInfo(v,flightsDataTransformer))).to("anomalies-topic", Produced.with(Serdes.String(), Serdes.String()));

        Topology topology = builder.build();

        KafkaStreams streams = new KafkaStreams(topology,props);
        streams.start();
    }

    public static String createOutputAnomaliesInfo(String val, FlightsDataTransformer flightsDataTransformer) {
        Airport airport = flightsDataTransformer.getAllAirports().get(val.substring(0,3));
        if (airport!=null) {
            val += " " + airport.getName() + " " + airport.getCity() + " " + airport.getState();
        }
        return val;
    }
}
