package com.redu.mapreduce.per;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author pengzhong
 * @since 2023/8/10
 */
public class MapOutValueComparator implements RawComparator<DimensionVO> {

    /*
     * 字节比较
     * b1,b2为要比较的两个字节数组
     * s1,l1表示第一个字节数组要进行比较的收尾位置，s2,l2表示第二个
     */
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        return WritableComparator.compareBytes(b1, 0, l1 - 4, b2, 0, l2 - 4);
    }

    @Override
    public int compare(DimensionVO o1, DimensionVO o2) {
        if (null != o1.getPlatform() && null != o2.getPlatform()) {
            int platformCmp = o1.getPlatform().compareTo(o2.getPlatform());
            if (0 == platformCmp) {
                int roleTypeCmp = o1.getRoleType() - o2.getRoleType();
                if (0 == roleTypeCmp) {
                    if (null != o1.getEmployeeNo() && null != o2.getEmployeeNo()) {
                        int employeeNoCmp = o1.getEmployeeNo().compareTo(o2.getEmployeeNo());
                        if (0 == employeeNoCmp) {
                            long userIdCmp = o1.getUserId() - o2.getUserId();
                            if (0 == userIdCmp) {
                                if (null != o1.getStatisticsTime() && null != o2.getStatisticsTime()) {
                                    return o1.getStatisticsTime().compareTo(o2.getStatisticsTime());
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
