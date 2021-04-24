import com.sun.scenario.effect.impl.sw.sse.SSEBlend_SRC_OUTPeer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.*;
import java.sql.Date;
import java.text.ParseException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class FlightsDataTransformer {

    private Map<String, Airport> allAirports;
    //pierwszy klucz - data, drugi klucz - stan
    private Map<String,Map<String, ReportInfo>> reportInfos;

    public void start() throws SQLException, FileNotFoundException {

        this.reportInfos = new HashMap<>();
        this.readAirports();

        Timer t = new Timer();
        UpdateTask updateTask = new UpdateTask(this);
        // This task is scheduled to run every 10 seconds

        t.scheduleAtFixedRate(updateTask, 0, 10000);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());



        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream("quickstart-events");
         source.foreach((k,v) -> {
             try {
                 apply(k,v);
             } catch (ParseException e) {
                 e.printStackTrace();
             }
         });

        Topology topology = builder.build();

        KafkaStreams streams = new KafkaStreams(topology,props);
        streams.start();
    }

    public static long getDateDiff(java.util.Date date1, java.util.Date date2, TimeUnit timeUnit) {
        long diffInMillies = date2.getTime() - date1.getTime();
        return timeUnit.convert(diffInMillies,TimeUnit.MILLISECONDS);
    }

    private void apply(String k, String v) throws ParseException {

        System.out.println(v);

        String[] flightData = v.split(",");

        String startAirport = flightData[3];
        String destAirport = flightData[4];
        String scheduledDeparture = flightData[5];
        String realDeparture = flightData[9];
        String scheduledArrival = flightData[8];
        String realArrival = flightData[13];
        String infoType = flightData[24];

        String startDate = scheduledDeparture.substring(0,10);
        String endDate = scheduledArrival.substring(0,10);

        if (this.reportInfos.get(startDate) == null) {
            this.reportInfos.put(startDate,new HashMap<>());
        }

        if (this.reportInfos.get(endDate) == null) {
            this.reportInfos.put(endDate,new HashMap<>());
        }

        if (infoType.equals("D")) {
            String stateDeparture = this.allAirports.get(startAirport).getState();
            int latencyInMinutes = calculateLatencyDeparture(realDeparture, scheduledDeparture);

            if (this.getReportInfos().get(startDate).get(stateDeparture) != null) {
                this.getReportInfos().get(startDate).get(stateDeparture).addDeparture();
                this.getReportInfos().get(startDate).get(stateDeparture).addLatencyDeparture(latencyInMinutes > 0 ? latencyInMinutes : 0);
            } else {
                this.getReportInfos().get(startDate).put(stateDeparture, new ReportInfo(stateDeparture,startDate, 1, 0, latencyInMinutes > 0 ? latencyInMinutes : 0,0));
            }
        } else if (infoType.equals("A")) {

            int latencyInMinutes = 0;
            if (realArrival.equals("\"\"") || scheduledArrival.equals("\"\"")) {
                latencyInMinutes = calculateLatencyArrival(realDeparture, scheduledDeparture);
            } else {
                latencyInMinutes = calculateLatencyArrival(realArrival, scheduledArrival);
            }

            String stateArrival = this.allAirports.get(destAirport).getState();

            if (this.getReportInfos().get(endDate).get(stateArrival) != null) {
                this.getReportInfos().get(endDate).get(stateArrival).addArrival();
                this.getReportInfos().get(endDate).get(stateArrival).addLatencyArrival(latencyInMinutes > 0 ? latencyInMinutes : 0);
            } else {
                this.getReportInfos().get(endDate).put(stateArrival, new ReportInfo(stateArrival, endDate, 0, 1, 0,latencyInMinutes > 0 ? latencyInMinutes : 0));
            }

        }
    }

    public void readAirports() throws FileNotFoundException {

        Map<String, Airport> allAirports = new HashMap<>();

        File airportsFile = new File("src/main/resources/airports.csv");
        Scanner myReader = new Scanner(airportsFile);
        myReader.nextLine();
        while (myReader.hasNextLine()) {
            String[] data = myReader.nextLine().split(",");
            Airport airportFromData = new Airport(Integer.parseInt(data[0]),data[1],data[4],data[2],data[13]);
            allAirports.put(airportFromData.getCodeIATA(),airportFromData);
        }
        myReader.close();

        this.allAirports = allAirports;
    }

    public static int calculateLatencyDeparture(String real, String scheduled) {
        Instant sd = Instant.parse(scheduled);
        java.util.Date dateSD = Date.from(sd);

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime dateRD = LocalDateTime.parse(real, formatter);
        Instant instantRD = dateRD.toInstant(ZoneOffset.UTC);
        java.util.Date dateRDD = Date.from(instantRD);

        return (int) ((dateRDD.getTime() - dateSD.getTime())/60000);
    }

    public static int calculateLatencyArrival(String real, String scheduled) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime dateSD = LocalDateTime.parse(scheduled, formatter);
        Instant instantSD = dateSD.toInstant(ZoneOffset.UTC);
        java.util.Date dateSDD = Date.from(instantSD);

        LocalDateTime dateRD = LocalDateTime.parse(real, formatter);
        Instant instantRD = dateRD.toInstant(ZoneOffset.UTC);
        java.util.Date dateRDD = Date.from(instantRD);

        return (int) ((dateRDD.getTime() - dateSDD.getTime()) / 60000);
    }


    public Map<String,Map<String, ReportInfo>> getReportInfos() {
        return reportInfos;
    }

    public void setReportInfos(Map<String,Map<String, ReportInfo>> reportInfos) {
        this.reportInfos = reportInfos;
    }
}
