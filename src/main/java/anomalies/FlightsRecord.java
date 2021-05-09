package anomalies;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;

public class FlightsRecord implements Serializable {

    private String airline;
    private String flightNumber;
    private String tailNumber;
    private String startAirport;
    private String destAirport;
    private String scheduledDepartureTime;
    private String scheduledDepartureDayOfWeek;
    private String scheduledFlightTime;
    private String scheduledArrivalTime;
    private String departureTime;
    private String taxiOut;
    private String distance;
    private String taxiIn;
    private String arrivalTime;
    private String diverted;
    private String cancelled;
    private String cancellationReason;
    private String airSystemDelay;
    private String securityDelay;
    private String airlineDelay;
    private String lateAircraftDelay;
    private String weatherDelay;
    private String cancelationTime;
    private String orderColumn;
    private String infoType;

    private String logline;

    private Instant startTime;
    private Instant endTime;

    public static long CURRENT_TIMESTAMP;

    public FlightsRecord() {}

    public FlightsRecord(String[] logline, String log) {
        this.setAirline(logline[0]);
        this.setFlightNumber(logline[1]);
        this.setTailNumber(logline[2]);
        this.setStartAirport(logline[3]);
        this.setDestAirport(logline[4]);
        this.setScheduledDepartureTime(logline[5]);
        this.setScheduledDepartureDayOfWeek(logline[6]);
        this.setScheduledFlightTime(logline[7]);
        this.setScheduledArrivalTime(logline[8]);
        this.setDepartureTime(logline[9]);
        this.setTaxiOut(logline[10]);
        this.setDistance(logline[11]);
        this.setTaxiIn(logline[12]);
        this.setArrivalTime(logline[13]);
        this.setDiverted(logline[14]);
        this.setCancelled(logline[15]);
        this.setCancellationReason(logline[16]);
        this.setAirSystemDelay(logline[17]);
        this.setSecurityDelay(logline[18]);
        this.setAirlineDelay(logline[19]);
        this.setLateAircraftDelay(logline[20]);
        this.setWeatherDelay(logline[21]);
        this.setCancelationTime(logline[22]);
        this.setOrderColumn(logline[23]);
        this.setInfoType(logline[24]);

        this.logline = log;

        CURRENT_TIMESTAMP = this.getTimestampInMillis();
    }

    public static boolean isOK(String logline) {
        return !logline.split(",")[0].equals("airline");
    }

    public static FlightsRecord parseFromLogLine(String logline) {
        return new FlightsRecord(logline.split(","), logline);
    }


    public String getAirline() {
        return airline;
    }

    public void setAirline(String airline) {
        this.airline = airline;
    }

    public String getFlightNumber() {
        return flightNumber;
    }

    public void setFlightNumber(String flightNumber) {
        this.flightNumber = flightNumber;
    }

    public String getTailNumber() {
        return tailNumber;
    }

    public void setTailNumber(String tailNumber) {
        this.tailNumber = tailNumber;
    }

    public String getStartAirport() {
        return startAirport;
    }

    public void setStartAirport(String startAirport) {
        this.startAirport = startAirport;
    }

    public String getDestAirport() {
        return destAirport;
    }

    public void setDestAirport(String destAirport) {
        this.destAirport = destAirport;
    }

    public String getScheduledDepartureTime() {
        return scheduledDepartureTime;
    }

    public void setScheduledDepartureTime(String scheduledDepartureTime) {
        this.scheduledDepartureTime = scheduledDepartureTime;
    }

    public String getScheduledDepartureDayOfWeek() {
        return scheduledDepartureDayOfWeek;
    }

    public void setScheduledDepartureDayOfWeek(String scheduledDepartureDayOfWeek) {
        this.scheduledDepartureDayOfWeek = scheduledDepartureDayOfWeek;
    }

    public String getScheduledFlightTime() {
        return scheduledFlightTime;
    }

    public void setScheduledFlightTime(String scheduledFlightTime) {
        this.scheduledFlightTime = scheduledFlightTime;
    }

    public String getScheduledArrivalTime() {
        return scheduledArrivalTime;
    }

    public void setScheduledArrivalTime(String scheduledArrivalTime) {
        this.scheduledArrivalTime = scheduledArrivalTime;
    }

    public String getDepartureTime() {
        return departureTime;
    }

    public void setDepartureTime(String departureTime) {
        this.departureTime = departureTime;
    }

    public String getTaxiOut() {
        return taxiOut;
    }

    public void setTaxiOut(String taxiOut) {
        this.taxiOut = taxiOut;
    }

    public String getDistance() {
        return distance;
    }

    public void setDistance(String distance) {
        this.distance = distance;
    }

    public String getTaxiIn() {
        return taxiIn;
    }

    public void setTaxiIn(String taxiIn) {
        this.taxiIn = taxiIn;
    }

    public String getArrivalTime() {
        return arrivalTime;
    }

    public void setArrivalTime(String arrivalTime) {
        this.arrivalTime = arrivalTime;
    }

    public String getDiverted() {
        return diverted;
    }

    public void setDiverted(String diverted) {
        this.diverted = diverted;
    }

    public String getCancelled() {
        return cancelled;
    }

    public void setCancelled(String cancelled) {
        this.cancelled = cancelled;
    }

    public String getCancellationReason() {
        return cancellationReason;
    }

    public void setCancellationReason(String cancellationReason) {
        this.cancellationReason = cancellationReason;
    }

    public String getAirSystemDelay() {
        return airSystemDelay;
    }

    public void setAirSystemDelay(String airSystemDelay) {
        this.airSystemDelay = airSystemDelay;
    }

    public String getSecurityDelay() {
        return securityDelay;
    }

    public void setSecurityDelay(String securityDelay) {
        this.securityDelay = securityDelay;
    }

    public String getAirlineDelay() {
        return airlineDelay;
    }

    public void setAirlineDelay(String airlineDelay) {
        this.airlineDelay = airlineDelay;
    }

    public String getLateAircraftDelay() {
        return lateAircraftDelay;
    }

    public void setLateAircraftDelay(String lateAircraftDelay) {
        this.lateAircraftDelay = lateAircraftDelay;
    }

    public String getWeatherDelay() {
        return weatherDelay;
    }

    public void setWeatherDelay(String weatherDelay) {
        this.weatherDelay = weatherDelay;
    }

    public String getCancelationTime() {
        return cancelationTime;
    }

    public void setCancelationTime(String cancelationTime) {
        this.cancelationTime = cancelationTime;
    }

    public String getOrderColumn() {
        return orderColumn;
    }

    public void setOrderColumn(String orderColumn) {
        this.orderColumn = orderColumn;
    }

    public String getInfoType() {
        return infoType;
    }

    public void setInfoType(String infoType) {
        this.infoType = infoType;
    }

    public long getTimestampInMillis() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        java.util.Date date;
        try {
            date = sdf.parse(orderColumn);
//            return date.getTime();
            return date.getTime() - 35*1000*60;
        } catch (ParseException e) {
            return -1;
        }
    }

    public long getDepartureInMillis() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        java.util.Date date;
        try {
            date = sdf.parse(departureTime);
            return date.getTime();
        } catch (ParseException e) {
            return -1;
        }
    }

    public long getArrivalInMillis() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        java.util.Date date;
        try {
            if (!arrivalTime.equals("")) {
                date = sdf.parse(arrivalTime);
            } else {
                date = sdf.parse(scheduledArrivalTime);
            }
            return date.getTime();
        } catch (ParseException e) {
            return -1;
        }
    }

    public boolean isDepartureTimeInInterval(int interval, int minutesBefore) {

        long max_timestamp = CURRENT_TIMESTAMP + interval*1000*60 + minutesBefore*1000*60;
        long min_timestamp = CURRENT_TIMESTAMP + minutesBefore*1000*60;

        return (getDepartureInMillis() >= min_timestamp && getDepartureInMillis() <= max_timestamp);
    }

    public boolean isArrivalTimeInInterval(int interval, int minutesBefore) {

        long max_timestamp = CURRENT_TIMESTAMP + interval*1000*60 + minutesBefore*1000*60;
        long min_timestamp = CURRENT_TIMESTAMP + minutesBefore*1000*60;

        return (getArrivalInMillis() >= min_timestamp && getArrivalInMillis() <= max_timestamp);
    }

    public boolean isArrivalInFuture() {
        return getArrivalInMillis() - CURRENT_TIMESTAMP >= 0;
    }

    public Instant getStartTime() {
        return startTime;
    }

    public void setStartTime(Instant startTime) {
        this.startTime = startTime;
    }

    public Instant getEndTime() {
        return endTime;
    }

    public void setEndTime(Instant endTime) {
        this.endTime = endTime;
    }

    @Override
    public String toString() {
        return this.logline;
    }
}
