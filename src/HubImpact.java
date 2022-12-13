/**
 * HubImpact Class contains setters/getters of hubID
 * and the impact(number of people who regain service per hour of repair) of a hub
 */

public class HubImpact {

    // internal attributes of the class
    private String hubID;
    private float impact;

    // constructor with parameters
    public HubImpact(String hubID, float impact) {
        this.hubID = hubID;
        this.impact = impact;
    }

    // Getters and Setters
    public String getHubID() {
        return hubID;
    }

    public void setHubID(String hubID) {
        this.hubID = hubID;
    }

    public float getImpact() {
        return impact;
    }

    public void setImpact(float impact) {
        this.impact = impact;
    }
}
