import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.*;
import java.util.*;

public class PowerService {

    public PowerService() {
        Connection connect = getDbConnection();
        try {
            Statement statement = connect.createStatement();
            statement.addBatch(Db.createPostalCodesTable());
            statement.addBatch(Db.createDistributionHubsTable());
            statement.addBatch(Db.createPostalCodeDistributionHubsTable());
            statement.addBatch(Db.createHubRepairTable());
            statement.executeBatch();
            statement.close();
            connect.close();
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }
    }


    public boolean addPostalCode (String postalCode, int population, int area ) {
        if (postalCode==null || postalCode.trim()=="" || population<=0 || area<=0) {
            throw new IllegalArgumentException();
        }

        String query = Db.addPostalCodeQuery(postalCode, population, area);
        Connection connect = getDbConnection();

        try {
            Statement statement = connect.createStatement();
            statement.executeUpdate(query);
            statement.close();
            connect.close();
            return true;
        } catch (SQLException e) {
            throw  new RuntimeException(e.getMessage());
        }
    }

    public boolean addDistributionHub ( String hubIdentifier, Point location, Set<String> servicedAreas ) {
        if (hubIdentifier==null || hubIdentifier.trim()=="" || location==null || servicedAreas==null) {
            throw new IllegalArgumentException();
        }

        String addDistributionHubQuery = Db.addDistributionHubQuery(hubIdentifier, location);
        String addPostalCodeDistributionHubQuery = Db.addPostalCodeDistributionHubQuery(hubIdentifier, servicedAreas);
        Connection connect = getDbConnection();

        try {
            Statement statement = connect.createStatement();
            String getPostalCodesQuery = Db.getPostalCodesQuery();
            ResultSet resultSet = statement.executeQuery(getPostalCodesQuery);
            Set curPostalCodes = new HashSet<>();
            while (resultSet.next()) {
                curPostalCodes.add(resultSet.getString(Db.POSTAL_CODE));
            }
            for (String postalCode:servicedAreas) {
                if (curPostalCodes.contains(postalCode)==false) {
                    throw new IllegalArgumentException("Invalid postal code:"+postalCode);
                }
            }
            statement.execute(addDistributionHubQuery);
            String removeEntriesOfHubFromPostalCodeDistributionHubQuery = Db.removeEntriesOfHubFromPostalCodeDistributionHubQuery(hubIdentifier);
            statement.addBatch(removeEntriesOfHubFromPostalCodeDistributionHubQuery);
            statement.addBatch(addPostalCodeDistributionHubQuery);
            statement.executeBatch();
            statement.close();
            connect.close();
            return true;
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    public void hubDamage ( String hubIdentifier, float repairEstimate ) {
        if (hubIdentifier==null || hubIdentifier.trim()=="" || repairEstimate<=0) {
            throw new IllegalArgumentException();
        }

        String setHubDamageQuery = Db.setHubDamageQuery(hubIdentifier, repairEstimate);
        Connection connect = getDbConnection();

        try {
            Statement statement = connect.createStatement();
            ResultSet resultSet = statement.executeQuery(Db.getDistributionHubsQuery());
            Set curHubs = new HashSet<>();
            while (resultSet.next()) {
                curHubs.add(resultSet.getString(Db.HUB_ID));
            }
            if (curHubs.contains(hubIdentifier)==false) {
                throw new IllegalArgumentException();
            }
            statement.execute(setHubDamageQuery);
            statement.close();
            connect.close();
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    public void hubRepair( String hubIdentifier, String employeeId, float repairTime, boolean inService ) {
        if (hubIdentifier==null || hubIdentifier.trim()=="" || employeeId==null || employeeId.trim()=="") {
            throw new IllegalArgumentException();
        }
        if (repairTime<=0) {
            throw new IllegalArgumentException();
        }
        Connection connect = getDbConnection();
        try {
            Statement statement = connect.createStatement();
            ResultSet resultSet = statement.executeQuery(Db.getDistributionHubsQuery());
            Set curHubs = new HashSet<>();
            while (resultSet.next()) {
                curHubs.add(resultSet.getString(Db.HUB_ID));
            }
            if (curHubs.contains(hubIdentifier)==false) {
                throw new IllegalArgumentException();
            }
            connect.setAutoCommit(false);
            statement.addBatch(Db.addHubRepairQuery(hubIdentifier, employeeId, repairTime, inService));
            if (inService) {
                statement.addBatch(Db.setHubInServiceQuery(hubIdentifier));
            }
            statement.executeBatch();
            connect.commit();
            connect.setAutoCommit(true);
            statement.close();
            connect.close();
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    public int peopleOutOfService () {
        Connection connect = getDbConnection();
        float peopleOutOfService = 0;
        try {
            Statement statement = connect.createStatement();
            ResultSet resultSet = statement.executeQuery(Db.peopleOutOfServiceQuery());
            while (resultSet.next()) {
                peopleOutOfService = Float.parseFloat(resultSet.getString(Db.PEOPLE_OUT_OF_SERVICE));
            }
            statement.close();
            connect.close();
            return (int)Math.ceil(peopleOutOfService);
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    public List<DamagedPostalCodes> mostDamagedPostalCodes (int limit ) {
        if (limit<=0) {
            throw new IllegalArgumentException();
        }
        Connection connect = getDbConnection();
        try {
            Statement statement = connect.createStatement();
            ResultSet resultSet = statement.executeQuery(Db.mostDamagedPostalCodesQuery(limit));
            List<DamagedPostalCodes> damagedPostalCodes = new ArrayList<>();
            DamagedPostalCodes damagedPostalCode;
            String postalCode = "";
            Float totalRepairs;
            while (resultSet.next()) {
                postalCode = resultSet.getString(Db.POSTAL_CODE);
                totalRepairs = Float.parseFloat(resultSet.getString(Db.TOTAL_REPAIRS));
                damagedPostalCode = new DamagedPostalCodes(postalCode, totalRepairs);
                damagedPostalCodes.add(damagedPostalCode);
            }
            statement.close();
            connect.close();
            return damagedPostalCodes;
        } catch (SQLException e) {
            throw  new RuntimeException(e.getMessage());
        }
    }

    public List<HubImpact> fixOrder ( int limit ) {
        if (limit<=0) {
            throw new IllegalArgumentException();
        }
        List<HubImpact> hubImpacts = fixOrder();
        return hubImpacts.subList(0, Math.min(limit, hubImpacts.size()));
    }

    public List<Integer> rateOfServiceRestoration ( float increment ) {
        if (increment<=0) {
            throw new IllegalArgumentException();
        }

        List<HubImpact> fixOrder = fixOrder();
        Map<String, Float> hubsRepairEstimatesMap = getHubsRepairEstimates();

        int peopleOutOfService = peopleOutOfService();
        int totalPopulation = getTotalPopulation();

        List<Integer> result = new ArrayList<>();
        result.add(0);
        float curPopulationRestored = (float) (totalPopulation-peopleOutOfService)/totalPopulation;
        float curIncrementSum = 0;
        float repairTime = 0;
        int hubIndex = 0;

        int x = (int)(curPopulationRestored/increment);
        curIncrementSum = x*increment;
        for (int i = 0; i < x; i++) {
            result.add((int)Math.ceil(repairTime));
        }

        String hubID = "";
        Float hubImpact;
        Float hubRepairEstimate;

        for (int i = 0; i < fixOrder.size(); i++) {
            hubID = fixOrder.get(i).getHubID();
            hubImpact = fixOrder.get(i).getImpact();
            hubRepairEstimate = hubsRepairEstimatesMap.get(hubID);
            curPopulationRestored += (hubImpact*hubRepairEstimate)/totalPopulation;
            repairTime += hubRepairEstimate;
            while ((curIncrementSum+increment)<=curPopulationRestored) {
                result.add((int)Math.ceil(repairTime));
                curIncrementSum += increment;
            }
        }
        if (result.size()<(Math.ceil(1/increment)+1)) {
            result.add(result.get(result.size()-1));
        }
        return  result;
    }

    public List<HubImpact> repairPlan ( String startHub, int maxDistance, float maxTime ) {
        return null;
    }

    public List<String> underservedPostalByPopulation ( int limit ) {
        if (limit<=0) {
            throw new IllegalArgumentException();
        }
        Connection connect = getDbConnection();
        try {
            Statement statement = connect.createStatement();
            ResultSet resultSet = statement.executeQuery(Db.underservedPostalByPopulationQuery(limit));
            List<String> postalCodes = new ArrayList<>();
            while (resultSet.next()) {
                postalCodes.add(resultSet.getString(Db.POSTAL_CODE));
            }
            statement.close();
            connect.close();
            return postalCodes;
        } catch (SQLException e) {
            throw  new RuntimeException(e.getMessage());
        }
    }

    public List<String> underservedPostalByArea ( int limit ) {
        if (limit<=0) {
            throw new IllegalArgumentException();
        }
        Connection connect = getDbConnection();
        try {
            Statement statement = connect.createStatement();
            ResultSet resultSet = statement.executeQuery(Db.underservedPostalByAreaQuery(limit));
            List<String> postalCodes = new ArrayList<>();
            while (resultSet.next()) {
                postalCodes.add(resultSet.getString(Db.POSTAL_CODE));
            }
            statement.close();
            connect.close();
            return postalCodes;
        } catch (SQLException e) {
            throw  new RuntimeException(e.getMessage());
        }
    }

    private List<HubImpact> fixOrder () {
        Connection connect = getDbConnection();
        try {
            Statement statement = connect.createStatement();
            ResultSet resultSet = statement.executeQuery(Db.fixOrderQuery());
            List<HubImpact> hubImpacts = new ArrayList<>();
            HubImpact hubImpact;
            String hubID = "";
            Float impact;
            while (resultSet.next()) {
                hubID = resultSet.getString(Db.HUB_ID);
                impact = Float.parseFloat(resultSet.getString(Db.HUB_IMPACT));
                hubImpact = new HubImpact(hubID, impact);
                hubImpacts.add(hubImpact);
            }
            statement.close();
            connect.close();
            return hubImpacts;
        } catch (SQLException e) {
            throw  new RuntimeException(e.getMessage());
        }
    }

    private int getTotalPopulation() {
        Connection connect = getDbConnection();
        int totalPopulation = 0;
        try {
            Statement statement = connect.createStatement();
            ResultSet resultSet = statement.executeQuery(Db.totalPopulationQuery());
            while (resultSet.next()) {
                totalPopulation = Integer.parseInt(resultSet.getString(Db.TOTAL_POPULATION));
            }
            statement.close();
            connect.close();
            return totalPopulation;
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    private Map<String, Float> getHubsRepairEstimates() {
        Connection connect = getDbConnection();
        Map<String, Float> hubsRepairEstimatesMap = new HashMap<>();
        try {
            Statement statement = connect.createStatement();
            ResultSet resultSet = statement.executeQuery(Db.hubsRepairEstimatesQuery());
            String hubID = "";
            Float repairEstimate;
            while (resultSet.next()) {
                hubID = resultSet.getString(Db.HUB_ID);
                repairEstimate = Float.parseFloat(resultSet.getString(Db.REPAIR_ESTIMATE));
                hubsRepairEstimatesMap.put(hubID, repairEstimate);
            }
            statement.close();
            connect.close();
            return hubsRepairEstimatesMap;
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    private Connection getDbConnection() {
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
            Connection connect = DriverManager.getConnection(path, username, password );
            return connect;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }
}
