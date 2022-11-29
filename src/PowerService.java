import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;

public class PowerService {

    public PowerService() {

        Properties identity = new Properties();
        String username = "";
        String password = "";
        String path = "";

        try {
            InputStream stream = new FileInputStream(Constants.PROPERTY_FILENAME);

            identity.load(stream);

            username = identity.getProperty(Constants.USERNAME);
            password = identity.getProperty(Constants.PASSWORD);
            path = identity.getProperty(Constants.DATABASE_PATH);
        } catch (Exception e) {
            return;
        }

        Connection connect = null;
        Statement statement = null;
        ResultSet resultSet = null;

        try {
            Class.forName(Constants.JDBC);

            connect = DriverManager.getConnection(path, username, password );
            statement = connect.createStatement();
            StringBuilder selectDbQuery = new StringBuilder();
            selectDbQuery.append("use ");
            selectDbQuery.append(identity.getProperty(Constants.USERNAME)).append(";");
            statement.execute(selectDbQuery.toString());
            System.out.println(Db.createPostalCodesTable());
            System.out.println(Db.createDistributionHubsTable());
            statement.addBatch(Db.createPostalCodesTable());
            statement.addBatch(Db.createDistributionHubsTable());
//            System.out.println(Db.createPostalCodeDistributionHubsTable());
            statement.addBatch(Db.createPostalCodeDistributionHubsTable());
            statement.executeBatch();
            resultSet = statement.executeQuery("select LastName from employees limit 3;");

            while (resultSet.next()) {
                System.out.println("Employee name: " + resultSet.getString("LastName"));
            }

            resultSet.close();
            statement.close();
            connect.close();
        } catch (Exception e) {
            System.out.println("yo2");
            System.out.println("Connection failed");
            System.out.println(e.getMessage());
        }
    }


    public boolean addPostalCode (String postalCode, int population, int area ) {
        if (postalCode==null || postalCode.trim()=="" || population<=0 || area<=0) {
            return false;
        }

        String query = getAddPostalCodeQuery(postalCode, population, area);

        Properties identity = new Properties();
        String username = "";
        String password = "";
        String path = "";

        try {
            InputStream stream = new FileInputStream(Constants.PROPERTY_FILENAME);

            identity.load(stream);

            username = identity.getProperty(Constants.USERNAME);
            password = identity.getProperty(Constants.PASSWORD);
            path = identity.getProperty(Constants.DATABASE_PATH);
        } catch (Exception e) {
            return false;
        }

        Connection connect = null;
        Statement statement = null;

        try {
            Class.forName(Constants.JDBC);

            connect = DriverManager.getConnection(path, username, password );
            statement = connect.createStatement();
            StringBuilder selectDbQuery = new StringBuilder();
            selectDbQuery.append("use ");
            selectDbQuery.append(identity.getProperty(Constants.USERNAME)).append(";");
            statement.execute(selectDbQuery.toString());
            statement.executeUpdate(query);
            statement.close();
            connect.close();
            return true;
        } catch (Exception e) {
            System.out.println("yo");
            System.out.println("Connection failed");
            System.out.println(e.getMessage());
        }
        return false;
    }

    public boolean addDistributionHub ( String hubIdentifier, Point location, Set<String> servicedAreas ) {
        if (hubIdentifier==null || hubIdentifier.trim()=="" || location==null || servicedAreas==null) {
            return false;
        }

        Properties identity = new Properties();
        String username = "";
        String password = "";
        String path = "";
        String getAddDistributionHubQuery = getAddDistributionHubQuery(hubIdentifier, location);
        String getAddPostalCodeDistributionHubQuery = getAddPostalCodeDistributionHubQuery(hubIdentifier, servicedAreas);

        try {
            InputStream stream = new FileInputStream(Constants.PROPERTY_FILENAME);

            identity.load(stream);

            username = identity.getProperty(Constants.USERNAME);
            password = identity.getProperty(Constants.PASSWORD);
            path = identity.getProperty(Constants.DATABASE_PATH);
        } catch (Exception e) {
            return false;
        }

        Connection connect = null;
        Statement statement = null;

        try {
            Class.forName(Constants.JDBC);

            connect = DriverManager.getConnection(path, username, password );
            connect.setAutoCommit(false);
            statement = connect.createStatement();
            StringBuilder selectDbQuery = new StringBuilder();
            selectDbQuery.append("use ");
            selectDbQuery.append(identity.getProperty(Constants.USERNAME)).append(";");
            statement.execute(selectDbQuery.toString());
            statement.addBatch(getAddDistributionHubQuery);
            statement.addBatch(getAddPostalCodeDistributionHubQuery);
            statement.executeBatch();
            connect.commit();
            statement.close();
            connect.setAutoCommit(true);
            connect.close();
            return true;
        } catch (Exception e) {
            System.out.println("yo1");
            System.out.println("Connection failed");
            System.out.println(e.getMessage());
        }
        return false;
    }

    public void hubDamage ( String hubIdentifier, float repairEstimate ) {

    }

    public void hubRepair( String hubIdentifier, String employeeId, float repairTime, boolean inService ) {

    }

    public int peopleOutOfService () {
    /*
    with t1 as
        select *
            from POSTAL_CODE_TABLE join POSTAL_CODES_DISTRIBUTION_HUBS_TABLE using(POSTAL_CODE)
            join DISTRIBUTION_HUB_TABLE using (hubID)
    with t2 as
        select postal_code, count(*) as total_hubs, population
            from t1
    with t3 as
        select postal_code, count(*) as hubs_out_of_service
            from t1 where in_service=false
    select
       sum((population*hubs_out_of_service))/total_hubs)
       from t2 join t3 using(postal_code)
     */

        return 0;
    }

    /*
    with t1 as
        select postal_code, sum(repair_estimate) as total_repairs
            from POSTAL_CODE_TABLE join POSTAL_CODES_DISTRIBUTION_HUBS_TABLE using(POSTAL_CODE)
            join DISTRIBUTION_HUB_TABLE using (hubID)
            group by postal_code
            order by total_repairs DESC
            limit limit

     */
    public List<DamagedPostalCodes> mostDamagedPostalCodes (int limit ) {
        return null;
    }

    public List<HubImpact> fixOrder ( int limit ) {
        return null;
    }

    public List<Integer> rateOfServiceRestoration ( float increment ) {
        return null;
    }

    public List<HubImpact> repairPlan ( String startHub, int maxDistance, float maxTime ) {
        return null;
    }

    public List<String> underservedPostalByPopulation ( int limit ) {
        return null;
    }

    public List<String> underservedPostalByArea ( int limit ) {
        return null;
    }

    private static String getAddPostalCodeQuery(String postalCode, int population, int area) {
        StringBuilder query = new StringBuilder();
        query.append("INSERT INTO ");
        query.append(Db.POSTAL_CODES_TABLE);
        query.append(" VALUES (");
        query.append("\"").append(postalCode).append("\", ");
        query.append(population).append(", ").append(area).append(")");
        query.append(" ON DUPLICATE KEY  UPDATE ");
        query.append(Db.POPULATION).append("=").append(population).append(", ");
        query.append(Db.AREA).append("=").append(area).append(";");
        return query.toString();
    }

    private static String getAddDistributionHubQuery(String hubIdentifier, Point location) {
        StringBuilder query = new StringBuilder();
        query.append("INSERT INTO ");
        query.append(Db.DISTRIBUTION_HUBS_TABLE);
        query.append(" VALUES (");
        query.append("\"").append(hubIdentifier).append("\", ");
        query.append(location.getX()).append(", ").append(location.getY()).append(")");
        query.append(" ON DUPLICATE KEY  UPDATE ");
        query.append(Db.LOCATION_X).append("=").append(location.getX()).append(", ");
        query.append(Db.LOCATION_Y).append("=").append(location.getY()).append(";");
        return query.toString();
    }

    private static String getAddPostalCodeDistributionHubQuery(String hubIdentifier, Set<String> servicedAreas) {
        StringBuilder query = new StringBuilder();
        query.append("INSERT IGNORE INTO ");
        query.append(Db.POSTAL_CODES_DISTRIBUTION_HUBS_TABLE);
        query.append(" (").append(Db.POSTAL_CODE).append(", ");
        query.append(Db.HUB_ID).append(")");
        query.append(" VALUES ");
        for (String postalCode : servicedAreas) {
            query.append("(\"").append(postalCode).append("\", ");
            query.append("\"").append(hubIdentifier).append("\"), ");
        }
        query.replace(query.length() - 2, query.length(), ";");
        return query.toString();
    }

    private static class Constants {
        private static final String JDBC = "com.mysql.cj.jdbc.Driver";
        private static final String PROPERTY_FILENAME = "db/power.prop";
        private static final String DATABASE_PATH = "path";
        private static final String USERNAME = "username";
        private static final String PASSWORD = "password";
    }

    private static class Db {
        private static final String POSTAL_CODES_TABLE = "postal_codes_table";
        private static final String POSTAL_CODE = "postal_code";
        private static final String POPULATION = "population";
        private static final String AREA = "area";
        private static final String DISTRIBUTION_HUBS_TABLE = "distribution_hubs_table";
        private static final String HUB_ID = "hub_id";
        private static final String LOCATION_X = "location_x";
        private static final String LOCATION_Y = "location_y";
        private static final String POSTAL_CODES_DISTRIBUTION_HUBS_TABLE = "postal_codes_distribution_hubs_table";
        private static final String ID = "id";


        private static String createPostalCodesTable() {
            return "CREATE TABLE IF NOT EXISTS " + POSTAL_CODES_TABLE +
                    "(" +
                    POSTAL_CODE + " VARCHAR(6) PRIMARY KEY," +
                    POPULATION + " INT," +
                    AREA + " INT" +
                    ");";
        }

        private static String createDistributionHubsTable() {
            return "CREATE TABLE IF NOT EXISTS " + DISTRIBUTION_HUBS_TABLE +
                    "(" +
                    HUB_ID + " VARCHAR(256) PRIMARY KEY," +
                    LOCATION_X + " INT," +
                    LOCATION_Y + " INT" +
                    ");";
        }

        private static String createPostalCodeDistributionHubsTable() {
            return "CREATE TABLE IF NOT EXISTS " + POSTAL_CODES_DISTRIBUTION_HUBS_TABLE +
                    "(" +
                    ID + " INT PRIMARY KEY AUTO_INCREMENT," +
                    POSTAL_CODE + " VARCHAR(6) NOT NULL," +
                    HUB_ID + " VARCHAR(256) NOT NULL," +
                    "FOREIGN KEY(" + POSTAL_CODE + ") REFERENCES " + POSTAL_CODES_TABLE + "(" + POSTAL_CODE + ")," +
                    "FOREIGN KEY(" + HUB_ID + ") REFERENCES " + DISTRIBUTION_HUBS_TABLE + "(" + HUB_ID + ")," +
                    "UNIQUE(" + POSTAL_CODE + ", " + HUB_ID + ")" +
                    ");";
        }


    }




}
