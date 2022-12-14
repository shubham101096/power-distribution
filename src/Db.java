import java.util.Set;

/**
 * Db class contains queries needed to insert/read/update the data from database.
 */
public class Db {

    // internal attributes of the class
    // strings used in queries
    public static final String POSTAL_CODES_TABLE = "POSTAL_CODES_TABLE";
    public static final String POSTAL_CODE = "POSTAL_CODE";
    public static final String POPULATION = "POPULATION";
    public static final String AREA = "AREA";
    public static final String DISTRIBUTION_HUBS_TABLE = "DISTRIBUTION_HUBS_TABLE";
    public static final String EMPLOYEES_TABLE = "EMPLOYEES_TABLE";
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

    // Make constructor private so no object of this class can be created
    private Db() {

    }

    /**
     * Query to create postal codes table
     * @return
     */
    public static  String createPostalCodesTable() {
        return "CREATE TABLE IF NOT EXISTS " + POSTAL_CODES_TABLE +
                "(" +
                POSTAL_CODE + " VARCHAR(6) PRIMARY KEY," +
                POPULATION + " INT NOT NULL," +
                AREA + " INT NOT NULL" +
                ");";
    }

    /**
     * Query to create distribution hubs table
     * @return
     */
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

    /**
     * Query to create postal codes and distribution hubs junction table
     * @return
     */
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

    /**
     * Query to create hub repair table
     * @return
     */
    public static  String createHubRepairTable() {
        return "CREATE TABLE IF NOT EXISTS " + HUB_REPAIR_TABLE +
                "(" +
                ID + " INT PRIMARY KEY AUTO_INCREMENT, " +
                HUB_ID + " VARCHAR(256) NOT NULL, " +
                EMPLOYEE_ID + " VARCHAR(256) NOT NULL, " +
                REPAIR_TIME  + " FLOAT NOT NULL," +
                IN_SERVICE + " BOOL NOT NULL, " +
                "TIME_STAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, "+
                "FOREIGN KEY(" + HUB_ID + ") REFERENCES " + DISTRIBUTION_HUBS_TABLE + "(" + HUB_ID + "), " +
                "FOREIGN KEY(" + EMPLOYEE_ID + ") REFERENCES " + EMPLOYEES_TABLE + "(" + EMPLOYEE_ID + ")" +
                ");";
    }

    /**
     * Query to add/update postal code in postal codes table
     * @return
     */
    public static  String addPostalCodeQuery(String postalCode, int population, int area) {
        return "INSERT INTO "+POSTAL_CODES_TABLE+"("+POSTAL_CODE+", "+POPULATION+", "+AREA+") VALUES\n" +
                "(\""+postalCode+"\", "+population+", "+area+")\n" +
                "ON DUPLICATE KEY UPDATE \n" +
                POPULATION+"="+population+",\n" +
                AREA+"="+area+";";
    }

    /**
     * Query to add/update distribution hub in distribution hubs table
     * @return
     */
    public static  String addDistributionHubQuery(String hubIdentifier, Point location) {
        return "INSERT INTO "+DISTRIBUTION_HUBS_TABLE+"("+HUB_ID+", "+LOCATION_X+", "+LOCATION_Y+") VALUES\n" +
                "(\""+hubIdentifier+"\", "+location.getX()+", "+location.getY()+")\n" +
                "ON DUPLICATE KEY UPDATE \n" +
                LOCATION_X+"="+location.getX()+",\n" +
                LOCATION_Y+"="+location.getY()+",\n" +
                IN_SERVICE+"=TRUE,\n" +
                REPAIR_ESTIMATE+"=0"+";";
    }

    /**
     * Query to add entry to postal codes and distribution hubs junction table
     * @return
     */
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

    /**
     * Query to fetch all info from postal codes table
     * @return
     */
    public static  String getPostalCodesQuery() {
        return "SELECT * from "+POSTAL_CODES_TABLE+";";
    }

    /**
     * Query to fetch all info from distribution hubs table
     * @return
     */
    public static  String getDistributionHubsQuery() {
        return "SELECT * from "+DISTRIBUTION_HUBS_TABLE+";";
    }

    /**
     * Query to fetch all info from employees table
     * @return
     */
    public static  String getEmployeesQuery() {
        return "SELECT * from "+EMPLOYEES_TABLE+";";
    }

    /**
     * Query to delete entries from postal codes and distribution hubs junction table having HUB_ID=hubID
     * @return
     */
    public static  String removeEntriesOfHubFromPostalCodeDistributionHubQuery(String hubID) {
        return "DELETE FROM "+POSTAL_CODES_DISTRIBUTION_HUBS_TABLE+
                "  WHERE "+HUB_ID+"=\""+hubID+"\";";
    }

    /**
     * Query to update estimated repair time of a hub
     * @return
     */
    public static  String setHubDamageQuery(String hubID, float repairEstimate) {
        return "UPDATE DISTRIBUTION_HUBS_TABLE\n" +
                "\tSET "+IN_SERVICE+"=FALSE, REPAIR_ESTIMATE="+repairEstimate+"\n" +
                " WHERE HUB_ID=\""+hubID+"\";";
    }

    /**
     * Query to add hub repair information to hub repair table
     * @return
     */
    public static  String addHubRepairQuery(String hubIdentifier, String employeeId, float repairTime, boolean inService) {
        return "INSERT INTO "+HUB_REPAIR_TABLE+"("+HUB_ID+", "+EMPLOYEE_ID+", "+REPAIR_TIME+", "+IN_SERVICE+")\n" +
                "VALUES (\""+hubIdentifier+"\", \""+employeeId+"\", "+repairTime+", "+inService+");";
    }

    /**
     * Querty to update status of hub to in service in distribution hubs table
     * @param hubID
     * @return
     */
    public static  String setHubInServiceQuery(String hubID) {
        return "UPDATE "+DISTRIBUTION_HUBS_TABLE+"\n" +
                "\tSET "+IN_SERVICE+"=TRUE, "+REPAIR_ESTIMATE+"=0\n" +
                "    WHERE "+HUB_ID+"=\""+hubID+"\";";
    }

    /**
     * Query to fetch total number of people who are out of service
     * @return
     */
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

    /**
     * Query to fetch information of limit number of most damaged postal codes
     * @return
     */
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

    /**
     * Query to fetch fix order of hubs that are out of service
     * @return
     */
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

    /**
     * Query to fetch total population in all postal codes
     * @return
     */
    public static  String totalPopulationQuery() {
        return "SELECT SUM("+POPULATION+") AS "+TOTAL_POPULATION+" FROM "+POSTAL_CODES_TABLE+";";
    }

    /**
     * Query to fetch hubs and their repair estimates from distribution hubs table
     * @return
     */
    public static  String hubsRepairEstimatesQuery() {
        return "SELECT "+HUB_ID+", "+REPAIR_ESTIMATE+" FROM "+DISTRIBUTION_HUBS_TABLE+";";
    }

    /**
     * Query to fetch information of a hub from distribution hubs table
     * @param hubID
     * @return
     */
    public static String getHubInfoQuery(String hubID) {
        return "SELECT * FROM "+DISTRIBUTION_HUBS_TABLE+"\n" +
                "WHERE "+HUB_ID+"='"+hubID+"';";
    }

    /**
     * Query to fetch faulty hubs within max distance from start hub
     * @param startHubInfo
     * @param maxDist
     * @return
     */
    public static String getFaultyHubsWithinMaxDistQuery(HubInfo startHubInfo, int maxDist) {
        return "SELECT * FROM "+DISTRIBUTION_HUBS_TABLE+"\n" +
                "WHERE "+HUB_ID+"!='"+startHubInfo.getHubID()+"' AND "+IN_SERVICE+"=FALSE AND\n" +
                "ABS("+LOCATION_X+"-"+startHubInfo.getLocationX()+") + ABS("+LOCATION_Y+"-"+startHubInfo.getLocationY()+") <= "+maxDist+";";
    }

    /**
     * Query to fetch limit number of most underserved postal codes by population
     * @param limit
     * @return
     */
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

    /**
     * Query to fetch limit number of most underserved postal codes by area
     * @param limit
     * @return
     */
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
