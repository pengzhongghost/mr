package com.redu.mapreduce.performance;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Field;

/**
 * @author pengzhong
 * @since 2023/8/3
 */

@Data
@NoArgsConstructor
@AllArgsConstructor
public class EmployeePerformanceVO implements Writable {

    private String teamName;

    private int teamId;

    private String branchName;

    private int branchId;

    private String groupName;

    private int groupId;

    private String deptIdPath;

    private String deptNamePath;

    private String employeeName;

    private String statisticsTime;

    private String platform;

    private long orderCount;

    private long fundOrderCount;

    private long validOrderNum;

    private String gmv;

    private String fundOrderGmv;

    private String validServiceIncome;

    private int roleType;

    private String employeeNo;

    private String orderAchievementSum;

    private String estimateServiceIncome;

    private long userId;

    private Long paidTime;

    private String paidTimeStr;

    private String serviceFeeRate;
    
    private String performanceNew;

    private String ds;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(null == teamName ? "" : teamName);
        dataOutput.writeInt(teamId);
        dataOutput.writeUTF(null == branchName ? "" : branchName);
        dataOutput.writeInt(branchId);
        dataOutput.writeUTF(null == groupName ? "" : groupName);
        dataOutput.writeInt(groupId);
        dataOutput.writeUTF(null == deptIdPath ? "" : deptIdPath);
        dataOutput.writeUTF(null == deptNamePath ? "" : deptNamePath);
        dataOutput.writeUTF(null == employeeName ? "" : employeeName);
        dataOutput.writeUTF(null == statisticsTime ? "" : statisticsTime);
        dataOutput.writeUTF(null == platform ? "" : platform);
        dataOutput.writeLong(orderCount);
        dataOutput.writeLong(fundOrderCount);
        dataOutput.writeLong(validOrderNum);
        dataOutput.writeUTF(null == gmv || "\\N".equals(gmv) ? "" : gmv);
        dataOutput.writeUTF(null == fundOrderGmv || "\\N".equals(fundOrderGmv) ? "" : fundOrderGmv);
        dataOutput.writeUTF(null == validServiceIncome || "\\N".equals(validServiceIncome) ? "" : validServiceIncome);
        dataOutput.writeInt(roleType);
        dataOutput.writeUTF(null == employeeNo ? "" : employeeNo);
        dataOutput.writeUTF(null == orderAchievementSum || "\\N".equals(orderAchievementSum) ? "" : orderAchievementSum);
        dataOutput.writeUTF(null == estimateServiceIncome || "\\N".equals(estimateServiceIncome) ? "" : estimateServiceIncome);
        dataOutput.writeLong(userId);
        dataOutput.writeUTF(paidTimeStr);
        dataOutput.writeUTF(serviceFeeRate);
        dataOutput.writeUTF(performanceNew);
        dataOutput.writeUTF(ds);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.teamName = dataInput.readUTF();
        this.teamId = dataInput.readInt();
        this.branchName = dataInput.readUTF();
        this.branchId = dataInput.readInt();
        this.groupName = dataInput.readUTF();
        this.groupId = dataInput.readInt();
        this.deptIdPath = dataInput.readUTF();
        this.deptNamePath = dataInput.readUTF();
        this.employeeName = dataInput.readUTF();
        this.statisticsTime = dataInput.readUTF();
        this.platform = dataInput.readUTF();
        this.orderCount = dataInput.readLong();
        this.fundOrderCount = dataInput.readLong();
        this.validOrderNum = dataInput.readLong();
        this.gmv = dataInput.readUTF();
        this.fundOrderGmv = dataInput.readUTF();
        this.validServiceIncome = dataInput.readUTF();
        this.roleType = dataInput.readInt();
        this.employeeNo = dataInput.readUTF();
        this.orderAchievementSum = dataInput.readUTF();
        this.estimateServiceIncome = dataInput.readUTF();
        this.userId = dataInput.readLong();
        this.paidTimeStr = dataInput.readUTF();
        this.serviceFeeRate = dataInput.readUTF();
        this.performanceNew = dataInput.readUTF();
        this.ds = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return
                teamName + "\u0001" +
                        teamId + "\u0001" +
                        branchName + "\u0001" +
                        branchId + "\u0001" +
                        groupName + "\u0001" +
                        groupId + "\u0001" +
                        deptIdPath + "\u0001" +
                        deptNamePath + "\u0001" +
                        employeeName + "\u0001" +
                        statisticsTime + "\u0001" +
                        platform + "\u0001" +
                        orderCount + "\u0001" +
                        fundOrderCount + "\u0001" +
                        validOrderNum + "\u0001" +
                        gmv + "\u0001" +
                        fundOrderGmv + "\u0001" +
                        validServiceIncome + "\u0001" +
                        roleType + "\u0001" +
                        employeeNo + "\u0001" +
                        orderAchievementSum + "\u0001" +
                        estimateServiceIncome + "\u0001" +
                        userId + "\u0001" +
                        performanceNew;
    }

    public static void main(String[] args) {
        Class aClass = EmployeePerformanceVO.class;
        StringBuilder outPut = new StringBuilder();
        StringBuilder intPut = new StringBuilder();
        for (Field declaredField : aClass.getDeclaredFields()) {
            if (declaredField.getType().equals(String.class)) {
                outPut.append("dataOutput.writeUTF(").append("null == ").append(declaredField.getName()).append(" ? \"\" : ").append(declaredField.getName()).append(");\n");
                intPut.append("this.").append(declaredField.getName()).append(" = ").append("dataInput.readUTF();\n");
            }
            if (declaredField.getType().equals(int.class)) {
                outPut.append("dataOutput.writeInt(").append(declaredField.getName()).append(");\n");
                intPut.append("this.").append(declaredField.getName()).append(" = ").append("dataInput.readInt();\n");
            }
            if (declaredField.getType().equals(long.class)) {
                outPut.append("dataOutput.writeLong(").append(declaredField.getName()).append(");\n");
                intPut.append("this.").append(declaredField.getName()).append(" = ").append("dataInput.readLong();\n");
            }
        }
        System.out.println(outPut);
        System.out.println(intPut);
    }

}
