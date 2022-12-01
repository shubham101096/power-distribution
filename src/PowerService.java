import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PowerService {

    private Db db;

    public PowerService() {
        db = Db.getInstance();
        Connection connect = db.getConnection();

        try {
            Statement statement = connect.createStatement();
            statement.addBatch(db.createPostalCodesTable());
            statement.addBatch(db.createDistributionHubsTable());
            statement.addBatch(db.createPostalCodeDistributionHubsTable());
            statement.addBatch(db.createHubRepairTable());
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

        String query = db.addPostalCodeQuery(postalCode, population, area);
        Connection connect = db.getConnection();

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

        String addDistributionHubQuery = db.addDistributionHubQuery(hubIdentifier, location);
        String addPostalCodeDistributionHubQuery = db.addPostalCodeDistributionHubQuery(hubIdentifier, servicedAreas);
        Connection connect = db.getConnection();

        try {
            Statement statement = connect.createStatement();
            String getPostalCodesQuery = db.getPostalCodesQuery();
            ResultSet resultSet = statement.executeQuery(getPostalCodesQuery);
            Set curPostalCodes = new HashSet<>();
            while (resultSet.next()) {
                curPostalCodes.add(resultSet.getString(db.POSTAL_CODE));
            }
            for (String postalCode:servicedAreas) {
                if (curPostalCodes.contains(postalCode)==false) {
                    throw new IllegalArgumentException("Invalid postal code:"+postalCode);
                }
            }
            statement.execute(addDistributionHubQuery);
            String removeEntriesOfHubFromPostalCodeDistributionHubQuery = db.removeEntriesOfHubFromPostalCodeDistributionHubQuery(hubIdentifier);
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

        String setHubDamageQuery = db.setHubDamageQuery(hubIdentifier, repairEstimate);
        Connection connect = db.getConnection();

        try {
            Statement statement = connect.createStatement();
            ResultSet resultSet = statement.executeQuery(db.getDistributionHubsQuery());
            Set curHubs = new HashSet<>();
            while (resultSet.next()) {
                curHubs.add(resultSet.getString(db.HUB_ID));
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
        Connection connect = db.getConnection();
        try {
            Statement statement = connect.createStatement();
            ResultSet resultSet = statement.executeQuery(db.getDistributionHubsQuery());
            Set curHubs = new HashSet<>();
            while (resultSet.next()) {
                curHubs.add(resultSet.getString(db.HUB_ID));
            }
            if (curHubs.contains(hubIdentifier)==false) {
                throw new IllegalArgumentException();
            }
            connect.setAutoCommit(false);
            statement.addBatch(db.addHubRepairQuery(hubIdentifier, employeeId, repairTime, inService));
            if (inService) {
                statement.addBatch(db.setHubInServiceQuery(hubIdentifier));
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
        Connection connect = db.getConnection();
        float peopleOutOfService = 0;
        try {
            Statement statement = connect.createStatement();
            ResultSet resultSet = statement.executeQuery(db.peopleOutOfServiceQuery());
            while (resultSet.next()) {
                peopleOutOfService = Float.parseFloat(resultSet.getString(db.PEOPLE_OUT_OF_SERVICE));
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
        Connection connect = db.getConnection();
        try {
            Statement statement = connect.createStatement();
            ResultSet resultSet = statement.executeQuery(db.mostDamagedPostalCodesQuery(limit));
            List<DamagedPostalCodes> damagedPostalCodes = new ArrayList<>();
            DamagedPostalCodes damagedPostalCode;
            String postalCode = "";
            Float totalRepairs;
            while (resultSet.next()) {
                postalCode = resultSet.getString(db.POSTAL_CODE);
                totalRepairs = Float.parseFloat(resultSet.getString(db.TOTAL_REPAIRS));
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
        Connection connect = db.getConnection();
        try {
            Statement statement = connect.createStatement();
            ResultSet resultSet = statement.executeQuery(db.fixOrderQuery(limit));
            List<HubImpact> hubImpacts = new ArrayList<>();
            HubImpact hubImpact;
            String hubID = "";
            Float impact;
            while (resultSet.next()) {
                hubID = resultSet.getString(db.HUB_ID);
                impact = Float.parseFloat(resultSet.getString(db.HUB_IMPACT));
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

}
