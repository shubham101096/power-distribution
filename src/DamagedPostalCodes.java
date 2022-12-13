/**
 * DamagedPostalCodes contains setters/getters of postalCode and
 * the total estimated repair time of all hubs that service it
 */

public class DamagedPostalCodes {

    // internal attributes of class
    private String postalCode;
    private Float repairEstimate;

    /**
     * Constructor with parameters
     * @param postalCode
     * @param repairEstimate
     */
    public DamagedPostalCodes(String postalCode, Float repairEstimate) {
        this.postalCode = postalCode;
        this.repairEstimate = repairEstimate;
    }

    // Getters and setters

    public String getPostalCode() {
        return postalCode;
    }

    public void setPostalCode(String postalCode) {
        this.postalCode = postalCode;
    }

    public Float getRepairEstimate() {
        return repairEstimate;
    }

    public void setRepairEstimate(Float repairEstimate) {
        this.repairEstimate = repairEstimate;
    }
}
