import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.*;
import java.util.*;

/**
 * PowerService Class handles data entry, reporting and planning methods related to
 * distribution hubs and postal codes
 */

public class PowerService {

    /**
     * Constructor
     */
    public PowerService() {
        /**
         * Create all required tables in db if not created yet
         */
        Connection connect = getDbConnection();

        try {
            Statement statement = connect.createStatement();
            statement.addBatch(Db.createPostalCodesTable());
            statement.addBatch(Db.createDistributionHubsTable());
            statement.addBatch(Db.createPostalCodeDistributionHubsTable());
            statement.addBatch(Db.createHubRepairTable());
            //it is assumed that employees table is already created
            // employees table would be created with the script attached with the project
            // and would be initialised with dummy values present in the script
            statement.executeBatch();
            statement.close();
            connect.close();
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    /**
     * Add a new postal code to db or update if already present
     * @param postalCode
     * @param population
     * @param area
     * @return true if successful
     */
    public boolean addPostalCode (String postalCode, int population, int area ) {
        // input validation
        if (postalCode==null || postalCode.trim()=="" || postalCode.length()>Constants.POSTAL_CODE_LENGTH || population<=0 || area<=0) {
            throw new IllegalArgumentException();
        }
        for (int i = 0; i < postalCode.length(); i++) {
            if (Character.isLetterOrDigit(postalCode.charAt(i))==false) {
                throw new IllegalArgumentException();
            }
        }

        // insert/update postalCode in db
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

    /**
     * Add a new distribution hub to db or update if already present
     * @param hubIdentifier
     * @param location
     * @param servicedAreas
     * @return true if successful
     */
    public boolean addDistributionHub ( String hubIdentifier, Point location, Set<String> servicedAreas ) {
        // input validation
        if (hubIdentifier==null || hubIdentifier.trim()=="" || location==null || servicedAreas==null) {
            throw new IllegalArgumentException();
        }
        for (int i = 0; i < hubIdentifier.length(); i++) {
            if (Character.isLetterOrDigit(hubIdentifier.charAt(i))==false) {
                throw new IllegalArgumentException();
            }
        }

        String addDistributionHubQuery = Db.addDistributionHubQuery(hubIdentifier, location);
        String addPostalCodeDistributionHubQuery = Db.addPostalCodeDistributionHubQuery(hubIdentifier, servicedAreas);
        Connection connect = getDbConnection();

        try {
            Statement statement = connect.createStatement();
            String getPostalCodesQuery = Db.getPostalCodesQuery();

            //fetch current postal codes in db
            ResultSet resultSet = statement.executeQuery(getPostalCodesQuery);
            Set curPostalCodes = new HashSet<>();
            while (resultSet.next()) {
                curPostalCodes.add(resultSet.getString(Db.POSTAL_CODE));
            }

            //check if postal codes in servicedAreas exist in db
            for (String postalCode:servicedAreas) {
                if (curPostalCodes.contains(postalCode)==false) {
                    throw new IllegalArgumentException("Invalid postal code:"+postalCode);
                }
            }
            // insert/update hub in db
            statement.execute(addDistributionHubQuery);
            String removeEntriesOfHubFromPostalCodeDistributionHubQuery = Db.removeEntriesOfHubFromPostalCodeDistributionHubQuery(hubIdentifier);

            //remove previously serviced areas of hub if any
            statement.addBatch(removeEntriesOfHubFromPostalCodeDistributionHubQuery);
            //add new serviced areas of hub
            statement.addBatch(addPostalCodeDistributionHubQuery);
            statement.executeBatch();
            statement.close();
            connect.close();
            return true;
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    /**
     * Report a damaged hub and estimated hours to repair it
     * @param hubIdentifier
     * @param repairEstimate
     */
    public void hubDamage ( String hubIdentifier, float repairEstimate ) {
        // input validation
        if (hubIdentifier==null || hubIdentifier.trim()=="" || repairEstimate<=0) {
            throw new IllegalArgumentException();
        }

        if (doesHubExistInDb(hubIdentifier)==false) {
            throw new IllegalArgumentException();
        }

        // insert/update repair estimate for a hub in db
        String setHubDamageQuery = Db.setHubDamageQuery(hubIdentifier, repairEstimate);
        Connection connect = getDbConnection();

        try {
            Statement statement = connect.createStatement();
            statement.execute(setHubDamageQuery);
            statement.close();
            connect.close();
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    /**
     * Report that given employee has done repair to the hub and whether it is in service now
     * @param hubIdentifier
     * @param employeeId
     * @param repairTime
     * @param inService
     */
    public void hubRepair( String hubIdentifier, String employeeId, float repairTime, boolean inService ) {
        // input validation
        if (hubIdentifier==null || hubIdentifier.trim()=="" || employeeId==null || employeeId.trim()=="") {
            throw new IllegalArgumentException();
        }
        if (repairTime<=0) {
            throw new IllegalArgumentException();
        }
        if (doesHubExistInDb(hubIdentifier)==false || doesEmployeeExistInDb(employeeId)==false) {
            throw new IllegalArgumentException();
        }

        Connection connect = getDbConnection();
        try {
            Statement statement = connect.createStatement();
            connect.setAutoCommit(false);
            // add hub repair info to db
            statement.addBatch(Db.addHubRepairQuery(hubIdentifier, employeeId, repairTime, inService));
            if (inService) {
                // update in db that hub is in service and its repair estimate is 0
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

    /**
     * Report number of people who are out of service
     * @return number of people who are out of service
     */
    public int peopleOutOfService () {
        Connection connect = getDbConnection();
        float peopleOutOfService = 0;
        try {
            Statement statement = connect.createStatement();
            //fetch number of people who are out of service from db
            ResultSet resultSet = statement.executeQuery(Db.peopleOutOfServiceQuery());
            //only 1 row with 1 column returned from the query
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

    /**
     * Report 'limit' number of postal codes and their repair estimate that need most repair
     * in descending order of repair time
     * @param limit
     * @return
     */
    public List<DamagedPostalCodes> mostDamagedPostalCodes (int limit ) {
        // input validation
        if (limit<=0) {
            throw new IllegalArgumentException();
        }
        Connection connect = getDbConnection();
        try {
            Statement statement = connect.createStatement();
            // fetch 'limit' number of postal codes and their repair estimate that need most repair
            // in descending order of repair time from db
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

    /**
     * Report 'limit' most significant hubs to fix and their impact in descending order where significance is number
     * of people who regain service per hour of repair
     * @param limit
     * @return
     */
    public List<HubImpact> fixOrder ( int limit ) {
        // input validation
        if (limit<=0) {
            throw new IllegalArgumentException();
        }
        // get all damaged hubs to fix in descending order of significance
        // (number of people who regain service per hour of repair)
        List<HubImpact> hubImpacts = fixOrder();
        // return first limit hubs
        return hubImpacts.subList(0, Math.min(limit, hubImpacts.size()));
    }

    /**
     * Report the estimate with which people are restored to power if hubs are fixed according to fixOrder
     * @param increment percentage of population
     * @return
     */
    public List<Integer> rateOfServiceRestoration ( float increment ) {
        // input validation
        if (increment<=0) {
            throw new IllegalArgumentException();
        }

        // get all damaged hubs to fix and their impact in descending order of significance
        // (number of people who regain service per hour of repair)
        List<HubImpact> fixOrder = fixOrder();

        // get repair estimate for each hub
        Map<String, Float> hubsRepairEstimatesMap = getHubsRepairEstimates();

        // get total people who are out of service
        int peopleOutOfService = peopleOutOfService();
        // get total population of province
        int totalPopulation = getTotalPopulation();

        List<Integer> result = new ArrayList<>(); //result is the list containing the final answer
        result.add(0); //time for 0% population to get to power

        // percenatge population already having power
        float curPopulationRestored = (float) (totalPopulation-peopleOutOfService)/totalPopulation;
        float curIncrementSum = 0;
        float repairTime = 0;
        int hubIndex = 0;

        // update result according to people already in power
        int x = (int)(curPopulationRestored/increment);
        curIncrementSum = x*increment;
        for (int i = 0; i < x; i++) {
            result.add((int)Math.ceil(repairTime));
        }

        String hubID = "";
        Float hubImpact;
        Float hubRepairEstimate;

        // update result as soon as the hubs get fixed in fix order

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

        // total entries in result should be Math.ceil(1/increment)+1
        // as we are using float the last entry of result may not get entered as
        //population restored may become 99.something % instead of exact 100%
        //so, we need to enter the last value of result if not already added
        if (result.size()<(Math.ceil(1/increment)+1)) {
            result.add(result.get(result.size()-1));
        }
        return  result;
    }

    /**
     *
     * @param startHub
     * @param maxDistance
     * @param maxTime
     * @return
     */
    public List<HubImpact> repairPlan ( String startHub, int maxDistance, float maxTime ) {
        // input validation
        if (startHub==null || startHub.trim()=="" || maxDistance<=0 || maxTime<=0) {
            throw new IllegalArgumentException();
        }
        if (doesHubExistInDb(startHub)==false) {
            throw new IllegalArgumentException();
        }

        HubInfo startHubInfo = getHubInfo(startHub);
        String startHubId = startHubInfo.getHubID();
        if (startHubInfo.isInService()) {
            // start hub should not be in service
            throw new IllegalArgumentException();
        }

        // get all damaged hubs to fix in descending order of significance
        // (number of people who regain service per hour of repair)
        List<HubImpact> fixOrder = fixOrder();

        // Get the HubInfo of all faulty hubs that are within max Distance (manhattan distance) from start hub
        List<HubInfo> faultyHubsWithinMaxDistList = new ArrayList<>(getFaultyHubsWithinMaxDist(startHubInfo, maxDistance));

        // also store the hubs found above in hashset for fast retrieval
        Set<String> faultyHubsWithinMaxDistSet = new HashSet<>();
        for (HubInfo hub : faultyHubsWithinMaxDistList) {
            faultyHubsWithinMaxDistSet.add(hub.getHubID());
        }

        String endHubId = "";
        HubInfo endHubInfo = new HubInfo();
        // find the most significant hub to fix from faultyHubsWithinMaxDistSet
        // this hub becomes the end hub
        for (HubImpact hubImpact : fixOrder) {
            if (faultyHubsWithinMaxDistSet.contains(hubImpact.getHubID())) {
                endHubId = hubImpact.getHubID();
                break;
            }
        }

        // find end HubInfo from end hubID which will be used later
        for (HubInfo hubInfo : faultyHubsWithinMaxDistList) {
            if (hubInfo.getHubID().equals(endHubId)) {
                endHubInfo = new HubInfo(hubInfo);
            }
        }

        // find all faulty hubs inside rectangle formed by start and end hub
        List<HubInfo> faultyHubsInsideRectangleList = new ArrayList<>();
        faultyHubsInsideRectangleList.add(startHubInfo);
        faultyHubsInsideRectangleList.addAll(getFaultyHubsInsideRectangle(startHubInfo, endHubInfo, faultyHubsWithinMaxDistList));

        // also store the hubs found above in hashset for fast retrieval
        Set<String> faultyHubsInsideRectangleSet = new HashSet<>();
        for (HubInfo hub : faultyHubsInsideRectangleList) {
            faultyHubsInsideRectangleSet.add(hub.getHubID());
        }

        // store the hubs and their impacts in a map for all the hubs in inside rectangle
        Map<String, HubImpact> hubImpactMap = new HashMap<>();

        for (HubImpact hubImpact : fixOrder) {
            if (faultyHubsInsideRectangleSet.contains(hubImpact.getHubID())) {
                hubImpactMap.put(hubImpact.getHubID(), hubImpact);
            }
        }

        if (faultyHubsWithinMaxDistList.size()==0) {
            // no hubs found within max distance from start hub
            // just return start hub
            List<HubImpact> list = new ArrayList<>();
            list.add(new HubImpact(startHubId, hubImpactMap.get(startHubId).getImpact()));
            return list;
        }

        // pass all info needed for finding repair path to the repairPlan.getRepairPath method
        RepairPlan repairPlan = new RepairPlan();
        return repairPlan.getRepairPath(startHubId, endHubId, faultyHubsInsideRectangleList, hubImpactMap, maxTime);
    }

    /**
     * Report 'limit' number of postal codes in descending order of services needed
     * (ascending order of avg number of hubs per person in a postal code)
     * @param limit
     * @return
     */
    public List<String> underservedPostalByPopulation ( int limit ) {
        // input validation
        if (limit<=0) {
            throw new IllegalArgumentException();
        }
        Connection connect = getDbConnection();
        try {
            Statement statement = connect.createStatement();
            // fetch postal codes in ascending order of avg number of hubs per person in that postal code
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

    /**
     *Report 'limit' number of postal codes in descending order of services needed
     *(ascending order of avg number of hubs per square meter in a postal code)
     * @param limit
     * @return
     */
    public List<String> underservedPostalByArea ( int limit ) {
        // input validation
        if (limit<=0) {
            throw new IllegalArgumentException();
        }
        Connection connect = getDbConnection();
        try {
            Statement statement = connect.createStatement();
            // fetch postal codes in ascending order of avg number of hubs square meter in that postal code
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

    /**
     * Get all the hubs to fix and their impact in descending order where order is determined by number
     * of people who regain service per hour of repair
     * @return
     */
    private List<HubImpact> fixOrder () {
        Connection connect = getDbConnection();
        try {
            Statement statement = connect.createStatement();
            // get fix order from db
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

    /**
     * Get the total population of province
     * @return total population
     */
    private int getTotalPopulation() {
        Connection connect = getDbConnection();
        int totalPopulation = 0;
        try {
            Statement statement = connect.createStatement();
            // get the total population of province from db
            ResultSet resultSet = statement.executeQuery(Db.totalPopulationQuery());
            // we get just one row and column from result set
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

    /**
     * Get repair estimates for all damaged hubs
     * @return map with key as hubID and value as repair estimate
     */
    private Map<String, Float> getHubsRepairEstimates() {
        Connection connect = getDbConnection();
        Map<String, Float> hubsRepairEstimatesMap = new HashMap<>();
        try {
            Statement statement = connect.createStatement();
            // get repair estimates for all damaged hubs
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

    /**
     * Get connection to db
     * @return Connection object having connection to db
     */
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

    /**
     * Check if hub exists in db
     * @param hubID
     * @return true if hub exists in db, false otherwise
     */
    private boolean doesHubExistInDb(String hubID) {
        Connection connect = getDbConnection();
        try {
            Statement statement = connect.createStatement();
            ResultSet resultSet = statement.executeQuery(Db.getDistributionHubsQuery());
            Set curHubs = new HashSet<>();
            while (resultSet.next()) {
                curHubs.add(resultSet.getString(Db.HUB_ID));
            }
            statement.close();
            connect.close();
            if (curHubs.contains(hubID)==false) {
                return false;
            }
            return true;
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    /**
     * Check if employee exists in db
     * @param employeeID
     * @return
     */
    private boolean doesEmployeeExistInDb(String employeeID) {
        Connection connect = getDbConnection();
        try {
            Statement statement = connect.createStatement();
            ResultSet resultSet = statement.executeQuery(Db.getEmployeesQuery());
            Set curEmployees = new HashSet<>();
            while (resultSet.next()) {
                curEmployees.add(resultSet.getString(Db.EMPLOYEE_ID));
            }
            statement.close();
            connect.close();
            if (curEmployees.contains(employeeID)==false) {
                return false;
            }
            return true;
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    /**
     * Get HubInfo object from hubID
     * @param hubID
     * @return
     */
    private HubInfo getHubInfo(String hubID) {
        Connection connect = getDbConnection();
        try {
            Statement statement = connect.createStatement();
            // fetch hub info from db
            ResultSet resultSet = statement.executeQuery(Db.getHubInfoQuery(hubID));
            int locationX = 0;
            int locationY = 0;
            boolean inService = true;
            float repairEstimate = 0;

            // resultSet contains one row
            while (resultSet.next()) {
                locationX = Integer.parseInt(resultSet.getString(Db.LOCATION_X));
                locationY = Integer.parseInt(resultSet.getString(Db.LOCATION_Y));
                inService = Boolean.parseBoolean(resultSet.getString(Db.IN_SERVICE));
                repairEstimate = Float.parseFloat(resultSet.getString(Db.REPAIR_ESTIMATE));
            }

            statement.close();
            connect.close();
            return new HubInfo(hubID, locationX, locationY, inService, repairEstimate);

        } catch (SQLException e) {
            throw  new RuntimeException(e.getMessage());
        }
    }

    /**
     * Get the HubInfo of all faulty hubs that are within max Distance (manhattan distance) from start hub
     * @param startHub
     * @param maxDist
     * @return list of HubInfo objects
     */
    private List<HubInfo> getFaultyHubsWithinMaxDist(HubInfo startHub, int maxDist) {
        Connection connect = getDbConnection();
        try {
            Statement statement = connect.createStatement();
            // fetch hub info of faulty hubs from db that are within max Distance (manhattan distance) from start hub
            ResultSet resultSet = statement.executeQuery(Db.getFaultyHubsWithinMaxDistQuery(startHub, maxDist));
            String hubID;
            int locationX = 0;
            int locationY = 0;
            boolean inService = true;
            float repairEstimate = 0;
            List faultyHubs = new ArrayList<>();

            while (resultSet.next()) {
                hubID = resultSet.getString(Db.HUB_ID);
                locationX = Integer.parseInt(resultSet.getString(Db.LOCATION_X));
                locationY = Integer.parseInt(resultSet.getString(Db.LOCATION_Y));
                inService = Boolean.parseBoolean(resultSet.getString(Db.IN_SERVICE));
                repairEstimate = Float.parseFloat(resultSet.getString(Db.REPAIR_ESTIMATE));
                faultyHubs.add(new HubInfo(hubID, locationX, locationY, inService, repairEstimate));
            }

            statement.close();
            connect.close();
            return faultyHubs;

        } catch (SQLException e) {
            throw  new RuntimeException(e.getMessage());
        }
    }

    /**
     * Get the HubInfo of all faulty hubs that are inside rectangle formed by start and end hub
     * excluding start hub
     * @param startHubInfo
     * @param endHubInfo
     * @param faultyHubsList
     * @return
     */
    private List<HubInfo> getFaultyHubsInsideRectangle(HubInfo startHubInfo, HubInfo endHubInfo, List<HubInfo> faultyHubsList) {

        List<HubInfo> faultyHubsInsideRectangleList =  new ArrayList<>();

        int minLocationX = Math.min(startHubInfo.getLocationX(), endHubInfo.getLocationX());
        int minLocationY = Math.min(startHubInfo.getLocationY(), endHubInfo.getLocationY());
        int maxLocationX = Math.max(startHubInfo.getLocationX(), endHubInfo.getLocationX());
        int maxLocationY = Math.max(startHubInfo.getLocationY(), endHubInfo.getLocationY());

        for (HubInfo hubInfo : faultyHubsList) {
           if (hubInfo.getLocationX()>=minLocationX && hubInfo.getLocationX()<=maxLocationX &&
                hubInfo.getLocationY()>=minLocationY && hubInfo.getLocationY()<=maxLocationY) {
                // include hub if it lies inside rectangle
                faultyHubsInsideRectangleList.add(hubInfo);
            }
        }

        return faultyHubsInsideRectangleList;
    }
}
