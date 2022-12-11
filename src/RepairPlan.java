import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RepairPlan {

    private class Constraints {
        boolean isXMonotonic = true;
        boolean isYMonotonic = true;
        boolean isXDirPos;
        boolean isYDirPos;
        boolean isDiagCrossed;

        Constraints(boolean isXMonotonic, boolean isYMonotonic, boolean isXDirPos, boolean isYDirPos, boolean isDiagCrossed) {
            this.isXMonotonic = isXMonotonic;
            this.isYMonotonic = isYMonotonic;
            this.isXDirPos = isXDirPos;
            this.isYDirPos = isYDirPos;
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
        boolean isXDirPos = (endHubInfo.getLocationX()-startHubInfo.getLocationX()>=0) ? true : false;
        boolean isYDirPos = (endHubInfo.getLocationY()-startHubInfo.getLocationY()>=0) ? true : false;
        Constraints constraints = new Constraints(true, true,isXDirPos, isYDirPos, false);
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
        boolean isVLinePositive = isPointLinePositive(line, pointV);

        for (int i = 1; i < faultyHubsList.size(); i++) {

            if (faultyHubsList.get(i).getHubID().equals(endHubID)) {
                if (curHubImpactSum>maxResult.maxHubImpactSum) {
                    maxResult.maxHubImpactSum = curHubImpactSum;
                    maxResult.maxImpactHubs = new ArrayList<>(curRepairOrderList);
                }
            } else if (isVisited[i]==false) {

                Point pointI = new Point(faultyHubsList.get(i).getLocationX(), faultyHubsList.get(i).getLocationY());
                boolean isILinePositive = isPointLinePositive(line, pointI);

                boolean isXGreater = (pointI.getX()-pointV.getX())>=0 ? true : false;
                boolean isYGreater = (pointI.getY()-pointV.getY())>=0 ? true : false;
                boolean isXMonotonic = isXGreater==constraints.isXDirPos ? true : false;
                boolean isYMonotonic = isYGreater==constraints.isYDirPos ? true : false;
                boolean isOnSameSideOfDiag = isVLinePositive==isILinePositive ? true : false;
                boolean isDiagCrossed = !isOnSameSideOfDiag;
                if (v==0) {
                    isDiagCrossed = false;
                }

                if (isXMonotonic==false && isYMonotonic==false) {
                    continue;
                }
                if (constraints.isDiagCrossed==true && isOnSameSideOfDiag==false) {
                    continue;
                }

                Constraints newConstraints = new Constraints(isXMonotonic, isYMonotonic, constraints.isXDirPos, constraints.isYDirPos, isDiagCrossed);
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

    private boolean isPointLinePositive(Line line, Point p) {
        int res = line.a* p.getX() + line.b* p.getY() - line.c;
        if (res>=0) {
            return true;
        }
        return false;
    }
}
