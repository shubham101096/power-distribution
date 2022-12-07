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
    public static final String TOTAL_POPULATION = "TOTAL_POPULATION";
    public static final String HUBS_OUT_OF_SERVICE = "HUBS_OUT_OF_SERVICE";
    public static final String TOTAL_HUBS ="TOTAL_HUBS";

    public static Db getInstance() {
        if (dbInstance == null) {
            dbInstance = new Db();
        }
        return dbInstance;
    }

    public static  String createPostalCodesTable() {
        return "CREATE TABLE IF NOT EXISTS " + POSTAL_CODES_TABLE +
                "(" +
                POSTAL_CODE + " VARCHAR(6) PRIMARY KEY," +
                POPULATION + " INT NOT NULL," +
                AREA + " INT NOT NULL" +
                ");";
    }

    public static  String createDistributionHubsTable() {
        return "CREATE TABLE IF NOT EXISTS " + DISTRIBUTION_HUBS_TABLE +
                "(" +
                HUB_ID + " VARCHAR(256) PRIMARY KEY," +
                LOCATION_X + " INT NOT NULL," +
                LOCATION_Y + " INT NOT NULL," +
                IN_SERVICE + " BOOL NOT NULL DEFAULT TRUE," +
                REPAIR_ESTIMATE + " FLOAT NOT NULL DEFAULT 0" +
                ");";
    }

    public static  String createPostalCodeDistributionHubsTable() {
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

    public static  String createHubRepairTable() {
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

    public static  String addPostalCodeQuery(String postalCode, int population, int area) {
        return "INSERT INTO "+POSTAL_CODES_TABLE+"("+POSTAL_CODE+", "+POPULATION+", "+AREA+") VALUES\n" +
                "(\""+postalCode+"\", "+population+", "+area+")\n" +
                "ON DUPLICATE KEY UPDATE \n" +
                POPULATION+"="+population+",\n" +
                AREA+"="+area+";";
    }

    public static  String addDistributionHubQuery(String hubIdentifier, Point location) {
        return "INSERT INTO "+DISTRIBUTION_HUBS_TABLE+"("+HUB_ID+", "+LOCATION_X+", "+LOCATION_Y+") VALUES\n" +
                "(\""+hubIdentifier+"\", "+location.getX()+", "+location.getY()+")\n" +
                "ON DUPLICATE KEY UPDATE \n" +
                LOCATION_X+"="+location.getX()+",\n" +
                LOCATION_Y+"="+location.getY()+";";
    }

    public static  String addPostalCodeDistributionHubQuery(String hubIdentifier, Set<String> servicedAreas) {
        StringBuilder query = new StringBuilder();
        query.append("INSERT INTO "+POSTAL_CODES_DISTRIBUTION_HUBS_TABLE+"("+POSTAL_CODE+", "+HUB_ID+") VALUES ");
        for (String postalCode : servicedAreas) {
            query.append("(\"").append(postalCode).append("\", ");
            query.append("\"").append(hubIdentifier).append("\"), ");
        }
        query.replace(query.length() - 2, query.length(), ";");
        return query.toString();
    }

    public static  String getPostalCodesQuery() {
        return "SELECT * from "+POSTAL_CODES_TABLE+";";
    }

    public static  String getDistributionHubsQuery() {
        return "SELECT * from "+DISTRIBUTION_HUBS_TABLE+";";
    }

    public static  String removeEntriesOfHubFromPostalCodeDistributionHubQuery(String hubID) {
        return "DELETE FROM "+POSTAL_CODES_DISTRIBUTION_HUBS_TABLE+
                "  WHERE "+HUB_ID+"=\""+hubID+"\";";
    }

    public static  String setHubDamageQuery(String hubID, float repairEstimate) {
        return "UPDATE DISTRIBUTION_HUBS_TABLE\n" +
                "\tSET "+IN_SERVICE+"=FALSE, REPAIR_ESTIMATE="+repairEstimate+"\n" +
                " WHERE HUB_ID=\""+hubID+"\";";
    }

    public static  String addHubRepairQuery(String hubIdentifier, String employeeId, float repairTime, boolean inService) {
        return "INSERT INTO "+HUB_REPAIR_TABLE+"("+HUB_ID+", "+EMPLOYEE_ID+", "+REPAIR_TIME+", "+IN_SERVICE+")\n" +
                "VALUES (\""+hubIdentifier+"\", \""+employeeId+"\", "+repairTime+", "+inService+");";
    }

    public static  String setHubInServiceQuery(String hubID) {
        return "UPDATE +"+DISTRIBUTION_HUBS_TABLE+"\n" +
                "\tSET +"+IN_SERVICE+"=TRUE, "+REPAIR_ESTIMATE+"=0\n" +
                "    WHERE HUB_ID=\"h1\";";
    }

    public static  String peopleOutOfServiceQuery() {
        return "WITH T1 AS\n" +
                "\t(SELECT *\n" +
                "\t\tFROM "+POSTAL_CODES_TABLE+" JOIN "+POSTAL_CODES_DISTRIBUTION_HUBS_TABLE+" USING("+POSTAL_CODE+")\n" +
                "\t\tJOIN "+DISTRIBUTION_HUBS_TABLE+" USING ("+HUB_ID+")),\n" +
                "T2 AS\n" +
                "\t(SELECT "+POSTAL_CODE+", COUNT(*) as TOTAL_HUBS, "+POPULATION+"\n" +
                "\t\tFROM T1\n" +
                "        GROUP BY "+POSTAL_CODE+"),\n" +
                "T3 AS\n" +
                "\t(SELECT "+POSTAL_CODE+", COUNT(*) as "+HUBS_OUT_OF_SERVICE+"\n" +
                "\t\tFROM T1 WHERE "+IN_SERVICE+"=FALSE\n" +
                "        GROUP BY "+POSTAL_CODE+")\n" +
                "SELECT COALESCE(SUM((POPULATION*"+HUBS_OUT_OF_SERVICE+")/"+TOTAL_HUBS+"), 0) AS "+PEOPLE_OUT_OF_SERVICE+"\n" +
                "   FROM T2 JOIN T3 USING("+POSTAL_CODE+");";
    }

    public static  String mostDamagedPostalCodesQuery(int limit) {
        String q = "SELECT "+POSTAL_CODE+", SUM("+REPAIR_ESTIMATE+") AS "+TOTAL_REPAIRS+"\n" +
                "\tFROM "+POSTAL_CODES_TABLE+" JOIN "+POSTAL_CODES_DISTRIBUTION_HUBS_TABLE+" USING("+POSTAL_CODE+")\n" +
                "\tJOIN "+DISTRIBUTION_HUBS_TABLE+" USING ("+HUB_ID+")\n" +
                "\tGROUP BY "+POSTAL_CODE+"\n" +
                "    HAVING "+TOTAL_REPAIRS+">0\n" +
                "\tORDER BY "+TOTAL_REPAIRS+" DESC\n" +
                "\tLIMIT "+limit+";";
        return q;
    }

    public static  String fixOrderQuery() {
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
                "SELECT "+HUB_ID+", "+REPAIR_ESTIMATE+", SUM(POPULATION_PER_HUB)/"+REPAIR_ESTIMATE+" AS "+HUB_IMPACT+
                " FROM T2 JOIN T3 USING("+POSTAL_CODE+") "+
                "GROUP BY "+HUB_ID+
                " ORDER BY "+HUB_IMPACT+" DESC;";
    }

    public static  String totalPopulationQuery() {
        return "SELECT SUM("+POPULATION+") AS "+TOTAL_POPULATION+" FROM "+POSTAL_CODES_TABLE+";";
    }

    public static  String hubsRepairEstimatesQuery() {
        return "SELECT "+HUB_ID+", "+REPAIR_ESTIMATE+" FROM "+DISTRIBUTION_HUBS_TABLE+";";
    }

    public static  String underservedPostalByPopulationQuery(int limit) {
        return "WITH T1 AS " +
                "(SELECT * "+
                "FROM "+POSTAL_CODES_TABLE+" JOIN "+POSTAL_CODES_DISTRIBUTION_HUBS_TABLE+" USING("+POSTAL_CODE+") "+
                "JOIN "+DISTRIBUTION_HUBS_TABLE+" USING("+HUB_ID+")) "+
                "SELECT "+POSTAL_CODE+"," + "COUNT(*)/"+POPULATION+" AS HUBS_PER_PERSON "+
                "FROM T1 GROUP BY "+POSTAL_CODE+" "+
                "ORDER BY HUBS_PER_PERSON "+
                "LIMIT "+limit+";";
    }

    public static  String underservedPostalByAreaQuery(int limit) {
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
