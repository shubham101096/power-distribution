import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RepairPlan {

    enum Value {
        NEGATIVE,
        ZERO,
        POSITIVE;

        static Value getValue(int n) {
            if (n<0) return NEGATIVE;
            if (n==0) return ZERO;
            return POSITIVE;
        }
    }

    private class Constraints {
        boolean isXMonotonic = true;
        boolean isYMonotonic = true;
        Value xDir;
        Value yDir;
        Value pointExistOnDiagonalSide;
        boolean isDiagCrossed;

        Constraints(boolean isXMonotonic, boolean isYMonotonic, Value xDir, Value yDir, Value pointExistOnDiagonalSide, boolean isDiagCrossed) {
            this.isXMonotonic = isXMonotonic;
            this.isYMonotonic = isYMonotonic;
            this.xDir = xDir;
            this.yDir = yDir;
            this.pointExistOnDiagonalSide = pointExistOnDiagonalSide;
            this.isDiagCrossed = isDiagCrossed;
        }
    }

    private class Line {
        int a;
        int b;
        int c;
    }

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

    String startHubID;
    String endHubID;
    List<HubInfo> faultyHubsList;
    Map<String, HubImpact> hubImpactMap;

    public RepairPlan(String startHubID, String endHubID, List<HubInfo> faultyHubsList, Map<String, HubImpact> hubImpactMap) {
//        Constraints constraints = new Constraints();
        this.startHubID = startHubID;
        this.endHubID = endHubID;
        this.faultyHubsList = new ArrayList<>(faultyHubsList);
        this.hubImpactMap = new HashMap<>(hubImpactMap);

    }

    public List<HubImpact> getRepairOrder() {
        List<HubImpact> repairOrderList = new ArrayList<>();
//        repairOrderList.add(hubImpactMap.get(startHubID));
        boolean isVisited[] = new boolean[faultyHubsList.size()];
        Result result =  new Result();
        Result tempResult;
        HubInfo startHubInfo = new HubInfo();
        HubInfo endHubInfo = new HubInfo();

        for (HubInfo hubInfo : faultyHubsList) {
            if (hubInfo.getHubID().equals(startHubID)) {
                startHubInfo = hubInfo;
            } else if (hubInfo.getHubID().equals(endHubID)) {
                endHubInfo = hubInfo;
            }
        }
        int diffX = endHubInfo.getLocationX()-startHubInfo.getLocationX();
        int diffY = endHubInfo.getLocationY()-startHubInfo.getLocationY();
        Value xDir = Value.getValue(diffX);
        Value yDir = Value.getValue(diffY);
        Constraints constraints = new Constraints(true, true, xDir, yDir, Value.ZERO, false);
        Point startPoint = new Point(startHubInfo.getLocationX(), startHubInfo.getLocationY());
        Point endPoint = new Point(endHubInfo.getLocationX(), endHubInfo.getLocationY());
        Line line = findDiagonalLineConstants(startPoint, endPoint);

        dfs(0, isVisited, 0, result, new ArrayList<>(), constraints, line);

//        for (int i = 0; i < faultyHubsList.size(); i++) {
//            dfs(i, isVisited, 0, result, new ArrayList<>());
////            if (tempResult.maxSum>result.maxSum) {
////                result = new Result(tempResult);
////            }
//        }

        repairOrderList.addAll(result.maxImpactHubs);
        repairOrderList.add(hubImpactMap.get(endHubID));
        return repairOrderList;
    }

    private void dfs(int v, boolean isVisited[], float curHubImpactSum, Result maxResult, List<HubImpact> curRepairOrderList, Constraints constraints, Line line) {
        isVisited[v] = true;
        String curHubID = faultyHubsList.get(v).getHubID();
        curHubImpactSum += hubImpactMap.get(curHubID).getImpact();
        curRepairOrderList.add(hubImpactMap.get(curHubID));
        Point pointV = new Point(faultyHubsList.get(v).getLocationX(), faultyHubsList.get(v).getLocationY());
        Value vValue = getPointValue(line, pointV);

        for (int i = 1; i < faultyHubsList.size(); i++) {

            if (faultyHubsList.get(i).getHubID().equals(endHubID)) {
                if (curHubImpactSum>maxResult.maxHubImpactSum) {
                    maxResult.maxHubImpactSum = curHubImpactSum;
                    maxResult.maxImpactHubs = new ArrayList<>(curRepairOrderList);
                }
            } else if (isVisited[i]==false) {

                Point pointI = new Point(faultyHubsList.get(i).getLocationX(), faultyHubsList.get(i).getLocationY());

                boolean isXMonotonic;
                boolean isYMonotonic;

                if (constraints.isXMonotonic==false) {
                    isXMonotonic = false;
                } else {
                    Value xCompare = Value.getValue(pointI.getX()-pointV.getX());
                    if (xCompare.equals(Value.ZERO)) {
                        isXMonotonic = constraints.isXMonotonic;
                    } else {
                        isXMonotonic = xCompare.equals(constraints.xDir);
                    }
                }

                if (constraints.isYMonotonic==false) {
                    isYMonotonic = false;
                } else {
                    Value yCompare = Value.getValue(pointI.getY()-pointV.getY());
                    if (yCompare.equals(Value.ZERO)) {
                        isYMonotonic = constraints.isYMonotonic;
                    } else {
                        isYMonotonic = yCompare.equals(constraints.yDir);
                    }
                }

//                boolean isOnSameSideOfDiag = isVLinePositive==isILinePositive ? true : false;
//                boolean isDiagCrossed = !isOnSameSideOfDiag;
//                if (v==0) {
//                    isDiagCrossed = false;
//                }

                if (isXMonotonic==false && isYMonotonic==false) {
                    continue;
                }
//                if (constraints.isDiagCrossed==true && isOnSameSideOfDiag==false) {
//                    continue;
//                }

                Value iValue = getPointValue(line, pointI);
                boolean willPathCrossDiagNow = false;
//
//                //diagonal already crossed
//                if (constraints.isDiagCrossed) {
//                    willPathCrossDiagNow = true;
//                } else {
//
//                    if (iValue.equals(Value.ZERO)) {
//                        willPathCrossDiagNow = false;
//                    }
//
//                    //diagonal not crossed yet
//                    if (vValue.equals(Value.ZERO)) {
//                        if (constraints.pointExistOnDiagonalSide.equals(iValue) == false) {
//                            willPathCrossDiagNow = true;
//                        }
//                    } else {
//                        if (vValue.equals(iValue)) {
//                            willPathCrossDiagNow = false;
//                        }
//                    }
//                }

                //is diagonal crossing now

                if (iValue.equals(vValue)==false) {
                    if (vValue.equals(Value.ZERO)) {
                        if (constraints.pointExistOnDiagonalSide.equals(Value.ZERO)==false && constraints.pointExistOnDiagonalSide.equals(iValue) == false) {
                                willPathCrossDiagNow = true;
                        }
                    } else {
                        willPathCrossDiagNow = true;
                    }
                }

                if (constraints.isDiagCrossed && willPathCrossDiagNow) {
                    continue;
                }

                boolean isDiagCrossed = willPathCrossDiagNow || constraints.isDiagCrossed;

                Value pointExistOnDiagonalSide;

                if (constraints.pointExistOnDiagonalSide.equals(Value.ZERO)) {
                    pointExistOnDiagonalSide = iValue;
                } else {
                    pointExistOnDiagonalSide = constraints.pointExistOnDiagonalSide;
                }
//
//                if (constraints.isDiagCrossed) {
//                    willPathCrossDiagNow = true;
//                } else if (iValue.equals(Value.ZERO)) {
//                    willPathCrossDiagNow = false;
//                } else if (vValue.equals(Value.ZERO)==false && constraints.pointExistOnDiagonalSide.equals(iValue)==false) {
//                    continue;
//                } else if () {
//
//                }

//
//                boolean willCrossDiagonal;
//                if (constraints.pointExistOnDiagonalSide)
//                boolean isOnSameSideOfDiag;
//                if (vValue==Value.ZERO || )
//                if (constraints.isDiagCrossed &&

                Constraints newConstraints = new Constraints(isXMonotonic, isYMonotonic, constraints.xDir, constraints.yDir, pointExistOnDiagonalSide, isDiagCrossed);
                dfs(i, isVisited, curHubImpactSum, maxResult, curRepairOrderList, newConstraints, line);

            }
        }
        curRepairOrderList.remove(curRepairOrderList.size()-1);
        isVisited[v] = false;
    }

    private Line findDiagonalLineConstants(Point p1, Point p2) {
        Line line = new Line();
        line.a = p2.getY()-p1.getY();
        line.b = p1.getX()-p2.getX();
        line.c = line.a*p1.getX() + line.b* p1.getY();
        return line;
    }

    private Value getPointValue(Line line, Point p) {
        int res = line.a* p.getX() + line.b* p.getY() - line.c;
        return Value.getValue(res);
    }
}