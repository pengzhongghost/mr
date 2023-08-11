package com.redu.mapreduce.test;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author pengzhong
 * @since 2023/8/4
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DimensionVO implements WritableComparable<DimensionVO> {

    private String platform;

    private int roleType;

    private String employeeNo;

    private long userId;

    private String statisticsTime;

    private long paidTime;

    @Override
    public String toString() {
        return "DimensionVO{" +
                "platform='" + platform + '\'' +
                ", roleType=" + roleType +
                ", employeeNo='" + employeeNo + '\'' +
                ", userId=" + userId +
                ", statisticsTime='" + statisticsTime + '\'' +
                '}';
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(null == platform ? "" : platform);
        dataOutput.writeInt(roleType);
        dataOutput.writeUTF(null == employeeNo ? "" : employeeNo);
        dataOutput.writeLong(userId);
        dataOutput.writeUTF(null == statisticsTime ? "" : statisticsTime);
        dataOutput.writeLong(paidTime);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.platform = dataInput.readUTF();
        this.roleType = dataInput.readInt();
        this.employeeNo = dataInput.readUTF();
        this.userId = dataInput.readLong();
        this.statisticsTime = dataInput.readUTF();
        this.paidTime = dataInput.readLong();
    }

        @Override
    public int compareTo(DimensionVO o) {
        if (null != platform && null != o.getPlatform()) {
            int platformCmp = platform.compareTo(o.getPlatform());
            if (0 == platformCmp) {
                int roleTypeCmp = this.roleType - o.roleType;
                if (0 == roleTypeCmp) {
                    if (null != employeeNo && null != o.getEmployeeNo()) {
                        int employeeNoCmp = employeeNo.compareTo(o.getEmployeeNo());
                        if (0 == employeeNoCmp) {
                            long userIdCmp = this.userId - o.getUserId();
                            if (0 == userIdCmp) {
                                if (null != statisticsTime && null != o.getStatisticsTime()) {
                                    int statisticTimeCmp = statisticsTime.compareTo(o.getStatisticsTime());
                                    if (0 == statisticTimeCmp) {
                                        return (int) (this.paidTime - o.getPaidTime());
                                    }
                                    return statisticTimeCmp;
                                }
                            }
                            return (int) userIdCmp;
                        }
                        return employeeNoCmp;
                    }
                }
                return roleTypeCmp;
            }
            return platformCmp;
        }
        return -1;
    }
}
