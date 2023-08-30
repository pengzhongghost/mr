package com.redu.mapreduce.per;

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

    //private String paidTimeStr;

    private String serviceFeeRate;

    /**
     * 入职时间
     */
    private String hiredDate;

    /**
     * 是否正式员工
     */
    private String isFormal;

    private Field[] fields;

    {
        Class<? extends EmployeePerformanceVO> aClass = this.getClass();
        fields = aClass.getDeclaredFields();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        for (Field field : fields) {
            Object value = null;
            try {
                value = field.get(this);
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
            if (String.class.equals(field.getType())) {
                dataOutput.writeUTF(String.valueOf(value));
            }
            if (int.class.equals(field.getType())) {
                dataOutput.writeInt((Integer) value);
            }
            if (long.class.equals(field.getType())) {
                dataOutput.writeLong((Long) value);
            }
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        for (Field field : fields) {
            if (String.class.equals(field.getType())) {
                try {
                    field.set(this, dataInput.readUTF());
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
            }
            if (int.class.equals(field.getType())) {
                try {
                    field.set(this, dataInput.readInt());
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
            }
            if (long.class.equals(field.getType())) {
                try {
                    field.set(this, dataInput.readLong());
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public String toString() {
        return
                teamName + "," +
                        teamId + "," +
                        branchName + "," +
                        branchId + "," +
                        groupName + "," +
                        groupId + "," +
                        deptIdPath + "," +
                        deptNamePath + "," +
                        employeeName + "," +
                        statisticsTime + "," +
                        platform + "," +
                        orderCount + "," +
                        fundOrderCount + "," +
                        validOrderNum + "," +
                        gmv + "," +
                        fundOrderGmv + "," +
                        validServiceIncome + "," +
                        roleType + "," +
                        employeeNo + "," +
                        orderAchievementSum + "," +
                        estimateServiceIncome + "," +
                        userId;
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
