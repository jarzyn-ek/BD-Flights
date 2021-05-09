package ETL;

public class ReportInfo {

    private String state;
    private int departuresToAdd;
    private int arrivalsToAdd;
    private int latencyDeparturesToAdd;
    private int latencyArrivalsToAdd;
    private String date;

    public ReportInfo(String state, String date, int departuresToAdd, int arrivalsToAdd, int latencyDeparturesToAdd, int latencyArrivalsToAdd) {
        this.state = state;
        this.setDate(date);
        this.departuresToAdd = departuresToAdd;
        this.arrivalsToAdd = arrivalsToAdd;
        this.latencyDeparturesToAdd = latencyDeparturesToAdd;
        this.latencyArrivalsToAdd = latencyArrivalsToAdd;
    }

    public void addArrival() {
        this.arrivalsToAdd += 1;
    }

    public void addDeparture() {
        this.departuresToAdd += 1;
    }

    public void addLatencyDeparture(int value) {
        this.latencyDeparturesToAdd += value;
    }

    public void addLatencyArrival(int value) {
        this.latencyArrivalsToAdd += value;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public int getDeparturesToAdd() {
        return departuresToAdd;
    }

    public void setDeparturesToAdd(int departuresToAdd) {
        this.departuresToAdd = departuresToAdd;
    }

    public int getArrivalsToAdd() {
        return arrivalsToAdd;
    }

    public void setArrivalsToAdd(int arrivalsToAdd) {
        this.arrivalsToAdd = arrivalsToAdd;
    }

    public int getLatencyDeparturesToAdd() {
        return latencyDeparturesToAdd;
    }

    public void setLatencyDeparturesToAdd(int latencyDeparturesToAdd) {
        this.latencyDeparturesToAdd = latencyDeparturesToAdd;
    }

    public int getLatencyArrivalsToAdd() {
        return latencyArrivalsToAdd;
    }

    public void setLatencyArrivalsToAdd(int latencyArrivalsToAdd) {
        this.latencyArrivalsToAdd = latencyArrivalsToAdd;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }
}
