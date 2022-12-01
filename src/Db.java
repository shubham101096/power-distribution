import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;
import java.util.Set;

public class Db {

    private static Db dbInstance;
    public static final String POSTAL_CODES_TABLE = "POSTAL_CODES_TABLE";
    public static final String POSTAL_CODE = "POSTAL_CODE";
    public static final String POPULATION = "POPULATION";
    public static final String AREA = "AREA";
    public static final String DISTRIBUTION_HUBS_TABLE = "DISTRIBUTION_HUBS_TABLE";
    public static final String HUB_REPAIR_TABLE = "HUB_REPAIR_TABLE";
    public static final String HUB_ID = "HUB_ID";
    public static final String LOCATION_X = "LOCATION_X";
    public static final String LOCATION_Y = "LOCATION_Y";
    public static final String IN_SERVICE = "IN_SERVICE";
    public static final String REPAIR_ESTIMATE = "REPAIR_ESTIMATE";
    public static final String POSTAL_CODES_DISTRIBUTION_HUBS_TABLE = "POSTAL_CODES_DISTRIBUTION_HUBS_TABLE";
    public static final String EMPLOYEE_ID = "EMPLOYEE_ID";
    public static final String REPAIR_TIME = "REPAIR_TIME";
    public static final String PEOPLE_OUT_OF_SERVICE = "PEOPLE_OUT_OF_SERVICE";
    public static final String TOTAL_REPAIRS = "TOTAL_REPAIRS";
    public static final String ID = "ID";
    public static final String HUB_IMPACT = "HUB_IMPACT";

    public static Db getInstance() {
        if (dbInstance == null) {
            dbInstance = new Db();
        }
        return dbInstance;
    }

    public Connection getConnection() {
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
            throw new RuntimeException(e.getMessage());
        }

        try {
            Class.forName(Constants.JDBC);
            Connection connect = connect = DriverManager.getConnection(path, username, password );
            return connect;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    public String createPostalCodesTable() {
        return "CREATE TABLE IF NOT EXISTS " + POSTAL_CODES_TABLE +
                "(" +
                POSTAL_CODE + " VARCHAR(6) PRIMARY KEY," +
                POPULATION + " INT NOT NULL," +
                AREA + " INT NOT NULL" +
                ");";
    }

    public String createDistributionHubsTable() {
        return "CREATE TABLE IF NOT EXISTS " + DISTRIBUTION_HUBS_TABLE +
                "(" +
                HUB_ID + " VARCHAR(256) PRIMARY KEY," +
                LOCATION_X + " INT NOT NULL," +
                LOCATION_Y + " INT NOT NULL," +
                IN_SERVICE + " BOOL NOT NULL DEFAULT TRUE," +
                REPAIR_ESTIMATE + " FLOAT NOT NULL DEFAULT 0" +
                ");";
    }

    public String createPostalCodeDistributionHubsTable() {
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

    public String createHubRepairTable() {
        return "CREATE TABLE IF NOT EXISTS " + HUB_REPAIR_TABLE +
                "(" +
                ID + " INT PRIMARY KEY AUTO_INCREMENT, " +
                HUB_ID + " VARCHAR(256) NOT NULL, " +
                EMPLOYEE_ID + " VARCHAR(256) NOT NULL, " +
                REPAIR_TIME  + " FLOAT NOT NULL," +
                IN_SERVICE + " BOOL NOT NULL, " +
                "FOREIGN KEY(" + HUB_ID + ") REFERENCES " + DISTRIBUTION_HUBS_TABLE + "(" + HUB_ID + ")" +
                ");";
    }

    public String addPostalCodeQuery(String postalCode, int population, int area) {
        StringBuilder query = new StringBuilder();
        query.append("INSERT INTO ");
        query.append(Db.POSTAL_CODES_TABLE);
        query.append(" VALUES (");
        query.append("\"").append(postalCode).append("\", ");
        query.append(population).append(", ").append(area).append(")");
        query.append(" ON DUPLICATE KEY UPDATE ");
        query.append(Db.POPULATION).append("=").append(population).append(", ");
        query.append(Db.AREA).append("=").append(area).append(";");
        return query.toString();
    }

    public String addDistributionHubQuery(String hubIdentifier, Point location) {
        StringBuilder query = new StringBuilder();
        query.append("INSERT INTO ");
        query.append(Db.DISTRIBUTION_HUBS_TABLE);
        query.append("(").append(HUB_ID).append(", ");
        query.append(LOCATION_X).append(", ");
        query.append(LOCATION_Y).append(")");
        query.append(" VALUES (");
        query.append("\"").append(hubIdentifier).append("\", ");
        query.append(location.getX()).append(", ").append(location.getY()).append(")");
        query.append(" ON DUPLICATE KEY  UPDATE ");
        query.append(Db.LOCATION_X).append("=").append(location.getX()).append(", ");
        query.append(Db.LOCATION_Y).append("=").append(location.getY()).append(";");
        return query.toString();
    }

    public String addPostalCodeDistributionHubQuery(String hubIdentifier, Set<String> servicedAreas) {
        StringBuilder query = new StringBuilder();
        query.append("INSERT INTO ");
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

    public String getPostalCodesQuery() {
        StringBuilder query = new StringBuilder();
        query.append("SELECT * FROM ");
        query.append(POSTAL_CODES_TABLE).append(";");
        return query.toString();
    }

    public String getDistributionHubsQuery() {
        StringBuilder query = new StringBuilder();
        query.append("SELECT * FROM ");
        query.append(DISTRIBUTION_HUBS_TABLE).append(";");
        return query.toString();
    }

    public String removeEntriesOfHubFromPostalCodeDistributionHubQuery(String hubID) {
        StringBuilder query = new StringBuilder();
        query.append("DELETE FROM ");
        query.append(POSTAL_CODES_DISTRIBUTION_HUBS_TABLE);
        query.append(" WHERE ").append(HUB_ID).append(" = ");
        query.append("\"").append(hubID).append("\"").append(";");
        return query.toString();
    }

    public String setHubDamageQuery(String hubID, float repairEstimate) {
        StringBuilder query = new StringBuilder();
        query.append("UPDATE ");
        query.append(DISTRIBUTION_HUBS_TABLE);
        query.append(" SET ");
        query.append(IN_SERVICE).append(" = FALSE, ");
        query.append(REPAIR_ESTIMATE).append(" = ").append(repairEstimate);
        query.append(" WHERE ").append(HUB_ID).append(" = ");
        query.append("\"").append(hubID).append("\"").append(";");
        return query.toString();
    }

    public String addHubRepairQuery(String hubIdentifier, String employeeId, float repairTime, boolean inService) {
        StringBuilder query = new StringBuilder();
        query.append("INSERT INTO ");
        query.append(Db.HUB_REPAIR_TABLE);
        query.append("(").append(HUB_ID).append(", ");
        query.append(EMPLOYEE_ID).append(", ");
        query.append(REPAIR_TIME).append(", ");
        query.append(IN_SERVICE).append(")");
        query.append(" VALUES (");
        query.append("\"").append(hubIdentifier).append("\", ");
        query.append(employeeId).append(", ").append(repairTime).append(", ").append(inService).append(");");
        return query.toString();
    }

    public String setHubInServiceQuery(String hubID) {
        StringBuilder query = new StringBuilder();
        query.append("UPDATE ")
                .append(DISTRIBUTION_HUBS_TABLE)
                .append(" SET ")
                .append(IN_SERVICE).append("=TRUE, ")
                .append(REPAIR_ESTIMATE).append("=0 ")
                .append(" WHERE ").append(HUB_ID).append(" = ")
                .append("\"").append(hubID).append("\"").append(";");
        return query.toString();
    }

    public String peopleOutOfServiceQuery() {
        StringBuilder query = new StringBuilder();
        query.append("WITH T1 AS ")
                .append("(SELECT * FROM ").append(POSTAL_CODES_TABLE).append(" JOIN ")
                .append(POSTAL_CODES_DISTRIBUTION_HUBS_TABLE).append(" USING(").append(POSTAL_CODE).append(") ")
                .append(" JOIN ").append(DISTRIBUTION_HUBS_TABLE).append(" USING(").append(HUB_ID).append(")), ")
                .append("T2 AS (SELECT ").append(POSTAL_CODE).append(", COUNT(*) AS TOTAL_HUBS, ").append(POPULATION)
                .append(" FROM T1 ").append("GROUP BY ").append(POPULATION).append("), ")
                .append(" T3 AS (SELECT ").append(POSTAL_CODE).append(", COUNT(*) AS HUBS_OUT_OF_SERVICE ")
                .append("FROM T1 WHERE ").append(IN_SERVICE).append("=FALSE ")
                .append("GROUP BY ").append(POSTAL_CODE).append(") ")
                .append("SELECT COALESCE(SUM((").append(POPULATION).append("*HUBS_OUT_OF_SERVICE)/TOTAL_HUBS), 0)")
                .append(" AS ").append(PEOPLE_OUT_OF_SERVICE)
                .append(" FROM T2 JOIN T3 USING(").append(POSTAL_CODE).append(");");
        return query.toString();
    }

    public String mostDamagedPostalCodesQuery(int limit) {
        StringBuilder query = new StringBuilder();
        query.append("SELECT ").append(POSTAL_CODE).append(", SUM(").append(REPAIR_ESTIMATE).append(") ")
                .append("AS ").append(TOTAL_REPAIRS)
                .append(" FROM ").append(POSTAL_CODES_TABLE).append(" JOIN ")
                .append(POSTAL_CODES_DISTRIBUTION_HUBS_TABLE).append(" USING(").append(POSTAL_CODE).append(") ")
                .append("JOIN ").append(DISTRIBUTION_HUBS_TABLE).append(" USING(").append(HUB_ID).append(") ")
                .append("GROUP BY ").append(POSTAL_CODE)
                .append(" HAVING ").append(TOTAL_REPAIRS).append(">0 ")
                .append("ORDER BY ").append(REPAIR_ESTIMATE).append(" DESC ")
                .append("LIMIT ").append(limit).append(";");
        return query.toString();
    }

    public String fixOrderQuery(int limit) {
        return "WITH T1 AS " +
                "(SELECT * "+
                "FROM "+POSTAL_CODES_TABLE+" JOIN "+POSTAL_CODES_DISTRIBUTION_HUBS_TABLE+" USING("+POSTAL_CODE+") "+
                "JOIN "+DISTRIBUTION_HUBS_TABLE+" USING("+HUB_ID+")), "+
                "T2 AS "+
                "(SELECT "+POSTAL_CODE+", "+POPULATION+"/COUNT(*) AS POPULATION_PER_HUB "+
                "FROM T1 GROUP BY "+POSTAL_CODE+"), "+
                "T3 AS "+
                "(SELECT "+HUB_ID+", "+POSTAL_CODE+", "+REPAIR_ESTIMATE+
                " FROM T1 WHERE "+IN_SERVICE+"=FALSE)"+
                "SELECT "+HUB_ID+", SUM(POPULATION_PER_HUB)/REPAIR_ESTIMATE AS "+HUB_IMPACT+
                " FROM T2 JOIN T3 USING("+POSTAL_CODE+") "+
                "GROUP BY "+HUB_ID+";";
    }

    public String underservedPostalByPopulationQuery(int limit) {
        return "WITH T1 AS " +
                "(SELECT * "+
                "FROM "+POSTAL_CODES_TABLE+" JOIN "+POSTAL_CODES_DISTRIBUTION_HUBS_TABLE+" USING("+POSTAL_CODE+") "+
                "JOIN "+DISTRIBUTION_HUBS_TABLE+" USING("+HUB_ID+")) "+
                "SELECT "+POSTAL_CODE+"," + "COUNT(*)/"+POPULATION+" AS HUBS_PER_PERSON "+
                "FROM T1 GROUP BY "+POSTAL_CODE+" "+
                "ORDER BY HUBS_PER_PERSON "+
                "LIMIT "+limit+";";
    }

    public String underservedPostalByAreaQuery(int limit) {
        return "WITH T1 AS " +
                "(SELECT * "+
                "FROM "+POSTAL_CODES_TABLE+" JOIN "+POSTAL_CODES_DISTRIBUTION_HUBS_TABLE+" USING("+POSTAL_CODE+") "+
                "JOIN "+DISTRIBUTION_HUBS_TABLE+" USING("+HUB_ID+")) "+
                "SELECT "+POSTAL_CODE+"," + "COUNT(*)/"+AREA+" AS HUBS_PER_AREA "+
                "FROM T1 GROUP BY "+POSTAL_CODE+" "+
                "ORDER BY HUBS_PER_AREA "+
                "LIMIT "+limit+";";
    }
}
