public class DamagedPostalCodes {

    String postalCode;
    Float repairEstimate;

    public DamagedPostalCodes(String postalCode, Float repairEstimate) {
        this.postalCode = postalCode;
        this.repairEstimate = repairEstimate;
    }

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
