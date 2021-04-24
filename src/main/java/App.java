import java.io.FileNotFoundException;
import java.sql.SQLException;

public class App {

    public static void main(String[] args) throws FileNotFoundException, SQLException {

        FlightsDataTransformer flightsDataTransformer = new FlightsDataTransformer();
        flightsDataTransformer.start();
    }
}
