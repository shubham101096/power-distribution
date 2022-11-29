import java.sql.*;
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
            statement.addBatch(Db.createPostalCodesTable());
            statement.addBatch(Db.createDistributionHubsTable());
            statement.addBatch(Db.createPostalCodeDistributionHubsTable());
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
            connect.setAutoCommit(false);
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
            statement.addBatch(addDistributionHubQuery);
            String removeEntriesOfHubFromPostalCodeDistributionHubQuery = db.removeEntriesOfHubFromPostalCodeDistributionHubQuery(hubIdentifier);
            statement.addBatch(removeEntriesOfHubFromPostalCodeDistributionHubQuery);
            statement.addBatch(addPostalCodeDistributionHubQuery);
            statement.executeBatch();
            connect.commit();
            statement.close();
            connect.setAutoCommit(true);
            connect.close();
            return true;
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }
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

}
