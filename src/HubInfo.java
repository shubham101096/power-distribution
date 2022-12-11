public class HubInfo {
    private String hubID;
    private int locationX;
    private int locationY;
    private boolean inService;
    private float repairEstimate;

    public HubInfo() {}

    public HubInfo(String hubID, int locationX, int locationY, boolean inService, float repairEstimate) {
        this.hubID = hubID;
        this.locationX = locationX;
        this.locationY = locationY;
        this.inService = inService;
        this.repairEstimate = repairEstimate;
    }

    public HubInfo(HubInfo hubInfo) {
        this.hubID = hubInfo.hubID;
        this.locationX = hubInfo.locationX;
        this.locationY = hubInfo.locationY;
        this.inService = hubInfo.inService;
        this.repairEstimate = hubInfo.repairEstimate;
    }

    public String getHubID() {
        return hubID;
    }

    public int getLocationX() {
        return locationX;
    }

    public int getLocationY() {
        return locationY;
    }

    public boolean isInService() {
        return inService;
    }

    public float getRepairEstimate() {
        return repairEstimate;
    }
}
