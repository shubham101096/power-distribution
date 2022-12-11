public class HubImpact {

    private String hubID;
    private float impact;



    public HubImpact(String hubID, float impact) {
        this.hubID = hubID;
        this.impact = impact;
    }

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
