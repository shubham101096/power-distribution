import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * RepairClass helps in finding the path(intermediate hubs to repair) from start hub to end hub
 */

public class RepairPlan {

    /**
     * Value enum is used to categorise int value
     */
    private enum Value {
        NEGATIVE,
        ZERO,
        POSITIVE;

        static Value getValue(int n) {
            if (n<0) return NEGATIVE;
            if (n==0) return ZERO;
            return POSITIVE;
        }
    }

    /**
     * Constraints contains variables that represent constraints for the repair plan
     */
    private class Constraints {
        float maxTime; // max time allowed at intermediate hubs
        boolean isXMonotonic; // is path traversed till now x monotonic
        boolean isYMonotonic; // is path traversed till now y monotonic
        Value xDir; // difference b/w end hub and start hub x-coordinate to represent x-direction in which we need to move to reach end hub
        Value yDir; // difference b/w end hub and start hub y-coordinate to represent y-direction in which we need to move to reach end hub

        /*
         sideOfPreviousNonDiagonalPoint helps in finding will diagonal be crossed now if
         current point is traversed.
         positive/negative value represents that on which side of the diagonal
         the closest previous non-diagonal(point which did not lie on the diagonal) point was traversed
         0 value means only the points that lie on diagonal have been traversed till now.
         */
        Value sideOfPreviousNonDiagonalPoint;

        boolean isDiagCrossed; // has diagonal been crossed in path traversed till now

        // constructor for constraints
        Constraints(float maxTime, boolean isXMonotonic, boolean isYMonotonic, Value xDir, Value yDir, Value sideOfPreviousNonDiagonalPoint, boolean isDiagCrossed) {
            this.maxTime = maxTime;
            this.isXMonotonic = isXMonotonic;
            this.isYMonotonic = isYMonotonic;
            this.xDir = xDir;
            this.yDir = yDir;
            this.sideOfPreviousNonDiagonalPoint = sideOfPreviousNonDiagonalPoint;
            this.isDiagCrossed = isDiagCrossed;
        }
    }

    /**
     * Line class is used to store line constants a, b, c of line equation
     * ax+by=c
     */
    private class Line {
        int a;
        int b;
        int c;

        // constructor that takes two points as arguments
        Line (Point p1, Point p2) {
            a = p2.getY()-p1.getY();
            b = p1.getX()-p2.getX();
            c = a*p1.getX() + b* p1.getY();
        }
    }

    /**
     * Result class is used to store the repair path having the max impact among all the traversed paths
     */
    private class Result {
        float maxHubImpactSum;
        List<HubImpact> maxImpactHubs;
        Result() {
            maxHubImpactSum = 0;
            maxImpactHubs = new ArrayList();
        }

        Result(Result c) {
            this.maxHubImpactSum = c.maxHubImpactSum;
            this.maxImpactHubs = new ArrayList<>(c.maxImpactHubs);
        }
    }

    /**
     * internal attributes of RepairPlan class
     */
    String startHubID;
    String endHubID;
    List<HubInfo> faultyHubsList; // all faulty hubs inside rectangle, first hub is the startHub in the list
    Map<String, HubImpact> hubImpactMap; // map storing hubs and their impacts

    public List<HubImpact> getRepairPath(String startHubID, String endHubID, List<HubInfo> faultyHubsList, Map<String, HubImpact> hubImpactMap, float maxTime) {

        this.startHubID = startHubID;
        this.endHubID = endHubID;
        this.faultyHubsList = new ArrayList<>(faultyHubsList);
        this.hubImpactMap = new HashMap<>(hubImpactMap);

        boolean isVisited[] = new boolean[faultyHubsList.size()];
        Result result =  new Result();
        HubInfo startHubInfo = new HubInfo();
        HubInfo endHubInfo = new HubInfo();

        // get start and end hub info
        for (HubInfo hubInfo : faultyHubsList) {
            if (hubInfo.getHubID().equals(startHubID)) {
                startHubInfo = hubInfo;
            } else if (hubInfo.getHubID().equals(endHubID)) {
                endHubInfo = hubInfo;
            }
        }
        int diffX = endHubInfo.getLocationX()-startHubInfo.getLocationX();
        int diffY = endHubInfo.getLocationY()-startHubInfo.getLocationY();

        // find direction between start and end hub
        Value xDir = Value.getValue(diffX);
        Value yDir = Value.getValue(diffY);

         /*
         initialize constrains
         initially:
         maxTime will be equal to maxTime and will never change
         isXMonotonic and isYMonotonic will be true
         xDir and yDir will never change
         sideOfPreviousNonDiagonalPoint will be 0 as start hub lies on diagonal
         isDiagCrossed will be false
          */
        Constraints constraints = new Constraints(maxTime, true, true, xDir, yDir, Value.ZERO, false);

        // get diagonal line constants from start and end points
        Point startPoint = new Point(startHubInfo.getLocationX(), startHubInfo.getLocationY());
        Point endPoint = new Point(endHubInfo.getLocationX(), endHubInfo.getLocationY());
        Line line = new Line(startPoint, endPoint);

        // call depth first search on the start hub
        dfs(0, isVisited, 0, 0, result, new ArrayList<>(), constraints, line);

        // return the final repair order list
        List<HubImpact> repairOrderList = new ArrayList<>();
        repairOrderList.addAll(result.maxImpactHubs);
        repairOrderList.add(hubImpactMap.get(endHubID));
        return repairOrderList;
    }

    /**
     * Depth first search is used to traverse the faultyHubsList by considering the constraints
     * @param v current hub
     * @param isVisited array containing whether hub has been visited
     * @param curHubImpactSum sum of impacts of hubs traversed till now
     * @param curTime time spent at intermediate hubs till now
     * @param maxResult Result storing repair path having the max impact among all the traversed paths
     * @param curRepairOrderList hubs currently in repair path
     * @param constraints constraints of the problem
     * @param diagonal Line representing diagonal of rectangle
     */
    private void dfs(int v, boolean isVisited[], float curHubImpactSum, float curTime, Result maxResult, List<HubImpact> curRepairOrderList, Constraints constraints, Line diagonal) {

        // mark v hub as visited
        isVisited[v] = true;
        String curHubID = faultyHubsList.get(v).getHubID();
        // add v to current hub list
        curHubImpactSum += hubImpactMap.get(curHubID).getImpact();
        curRepairOrderList.add(hubImpactMap.get(curHubID));
        Point pointV = new Point(faultyHubsList.get(v).getLocationX(), faultyHubsList.get(v).getLocationY());
        // determine on which side of diagonal hub v exists
        Value vSide = getPointSideOnLine(diagonal, pointV);

        // traverse other hubs in list
        for (int i = 1; i < faultyHubsList.size(); i++) {

            if (faultyHubsList.get(i).getHubID().equals(endHubID)) {
                // if hub is end hub then check if it is the best path

                if (curHubImpactSum>maxResult.maxHubImpactSum) {
                    maxResult.maxHubImpactSum = curHubImpactSum;
                    maxResult.maxImpactHubs = new ArrayList<>(curRepairOrderList);
                }
            } else if (isVisited[i]==false) {

                //ignore hub if maxTime exceeds
                if ((curTime+faultyHubsList.get(i).getRepairEstimate())>constraints.maxTime) {
                    continue;
                }

                Point pointI = new Point(faultyHubsList.get(i).getLocationX(), faultyHubsList.get(i).getLocationY());

                boolean isXMonotonicNow;
                boolean isYMonotonicNow;

                //check for xMonotonic
                if (constraints.isXMonotonic==false) {
                    // path is already not xMonotonic
                    isXMonotonicNow = false;
                } else {
                    // path is xMonotonic till before traversing i

                    Value xCompare = Value.getValue(pointI.getX()-pointV.getX());
                    if (xCompare.equals(Value.ZERO)) {
                        // hubs v and i have same x, so xMonotonic property remains as before
                        isXMonotonicNow = constraints.isXMonotonic;
                    } else {
                        // xMonotonic depends on if path from v to i is in xDir or not
                        isXMonotonicNow = xCompare.equals(constraints.xDir);
                    }
                }

                //check for yMonotonic
                if (constraints.isYMonotonic==false) {
                    // path is already not yMonotonic
                    isYMonotonicNow = false;
                } else {
                    // path is yMonotonic till before traversing i
                    Value yCompare = Value.getValue(pointI.getY()-pointV.getY());
                    if (yCompare.equals(Value.ZERO)) {
                        // hubs v and i have same y, so yMonotonic property remains as before
                        isYMonotonicNow = constraints.isYMonotonic;
                    } else {
                        // xMonotonic depends on if path from v to i is in xDir or not
                        isYMonotonicNow = yCompare.equals(constraints.yDir);
                    }
                }

                // ignore hub i if both property fail
                if (isXMonotonicNow==false && isYMonotonicNow==false) {
                    continue;
                }

                // determine on which side of diagonal hub i exists
                Value iSide = getPointSideOnLine(diagonal, pointI);
                boolean willPathCrossDiagNow = false;

                // check for diagonal constraint
                //is diagonal crossing now?

                if (iSide.equals(Value.ZERO)==false && iSide.equals(vSide)==false) {
                    // hub i does not lie on diagonal and hubs i and v have different values
                    // so, diagonal may get crossed
                    if (vSide.equals(Value.ZERO)) {
                        // hub v lies on diagonal
                        // so diagonal will be crossed only if the immediate previous non-diagonal hub traversed was on opposite side to hub i
                        if (constraints.sideOfPreviousNonDiagonalPoint.equals(Value.ZERO)==false && constraints.sideOfPreviousNonDiagonalPoint.equals(iSide) == false) {
                                willPathCrossDiagNow = true;
                        }
                    } else {
                        // hubs i and v lie on different side of diagonal
                        willPathCrossDiagNow = true;
                    }
                }

                // ignore hub if causes the diagonal to be crossed again
                if (constraints.isDiagCrossed && willPathCrossDiagNow) {
                    continue;
                }

                boolean isDiagCrossedNow = willPathCrossDiagNow || constraints.isDiagCrossed;

                Value sideOfPreviousNonDiagonalPointNow;

                if (iSide.equals(Value.ZERO)) {
                    // if hub i lies on diagonal, keep value as before
                    sideOfPreviousNonDiagonalPointNow = constraints.sideOfPreviousNonDiagonalPoint;
                } else {
                    // update to side of hub i
                    sideOfPreviousNonDiagonalPointNow = iSide;
                }

                // setup new constraints before recursive call to the hub i
                // constraints maxTime, xDir, yDir always remain same as set in the beginning
                Constraints newConstraints = new Constraints(constraints.maxTime, isXMonotonicNow, isYMonotonicNow, constraints.xDir, constraints.yDir, sideOfPreviousNonDiagonalPointNow, isDiagCrossedNow);
                dfs(i, isVisited, curHubImpactSum, curTime+faultyHubsList.get(i).getRepairEstimate(), maxResult, curRepairOrderList, newConstraints, diagonal);

            }
        }
        // before getting removed from stack, bring curRepairOrderList and isVisited to previous state
        curRepairOrderList.remove(curRepairOrderList.size()-1);
        isVisited[v] = false;
    }

    /**
     * Find on which side of line does point exists
     * @param line
     * @param p
     * @return Value (NEGATIVE, ZERO, POSITIVE)
     */
    private Value getPointSideOnLine(Line line, Point p) {
        int res = line.a* p.getX() + line.b* p.getY() - line.c;
        return Value.getValue(res);
    }
}
