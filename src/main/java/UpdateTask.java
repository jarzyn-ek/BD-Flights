import java.sql.*;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.TimerTask;

public class UpdateTask extends TimerTask {

    private FlightsDataTransformer flightsDataTransformer;
    private Connection con;

    public UpdateTask(FlightsDataTransformer flightsDataTransformer) throws SQLException {
        this.flightsDataTransformer = flightsDataTransformer;
        this.con = DriverManager.getConnection("jdbc:mysql://172.17.0.1:32768/FLIGHTS","root","my-secret-pw");
    }

    @Override
    public void run() {

        String toDelete = "";

        if (flightsDataTransformer.getReportInfos().keySet().size() >= 3) {
            for (String key : flightsDataTransformer.getReportInfos().keySet()) {
                int smaller = 0;
                for (String keyToCompare : flightsDataTransformer.getReportInfos().keySet()) {
                    smaller += (keyToCompare.compareTo(key) > 0 ? 1 : 0);
                    if (smaller == 2) {
                        toDelete = key;
                        break;
                    }
                }
            }
        }

        if (toDelete != "") {
            flightsDataTransformer.getReportInfos().remove(toDelete);
        }

        String query = " replace into FLIGHTS_DATA (STATE, DATE, DEPARTURES, DEPARTURES_DELAY, ARRIVALS, ARRIVALS_DELAY)"
                + " values (?, ?, ?, ?, ?, ?)";
        PreparedStatement preparedStmt = null;
        try {
            preparedStmt = con.prepareStatement(query);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

        for (Map<String, ReportInfo> map : flightsDataTransformer.getReportInfos().values()) {
            for (ReportInfo reportInfo : map.values()) {

                try {
                    preparedStmt.setString(1, reportInfo.getState());
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
                try {
                    preparedStmt.setDate(2, Date.valueOf(reportInfo.getDate()));
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
                try {
                    preparedStmt.setInt(3, reportInfo.getDeparturesToAdd());
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
                try {
                    preparedStmt.setInt(4, reportInfo.getLatencyDeparturesToAdd());
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
                try {
                    preparedStmt.setInt(5, reportInfo.getArrivalsToAdd());
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
                try {
                    preparedStmt.setInt(6, reportInfo.getLatencyArrivalsToAdd());
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
                try {
                    preparedStmt.addBatch();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }

            }
            if (!map.values().isEmpty()) {
                try {
                    preparedStmt.executeBatch();
                    System.out.println("EXECUTED");
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
        }
    }
}
