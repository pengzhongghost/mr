package com.redu.mapreduce.per.vo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

/**
 * @author pengzhong
 * @since 2023/8/25
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ReduOrderVO {

    private Long platformOrderId;
    private Byte platformType;
    private Byte orderStatus;
    private String appletPeg;
    private Byte secondInstitutionPeg;
    private Date paidTime;
    private Date settlementTime;
    private Date refundTime;
    private Date deliveryTime;
    private Date recvTime;
    private Date platformUpdateTime;
    private Date createTime;
    private Date updateTime;
    private Integer creatorId;
    private Integer modifierId;
    private Byte isDelete;
    private Long institutionId;
    private Long secondInstitutionId;
    private Long platformProductId;
    private Integer buyCount;
    private Long productId;
    private Long activityId;
    private Long secondActivityId;
    private Double payAmount;
    private Double estimateSettlementAmount;
    private Double settlementAmount;
    private Double serviceRate;
    private Double estimateServiceIncome;
    private Double realServiceIncome;
    private Double secondRealCommission;
    private Double secondEstimatedCommission;
    private Double secondCommissionRate;
    private Double techServiceFee;
    private Double techServiceFeeRate;
    private Double settledTechServiceFee;
    private Byte systemMultiple;
    private Double weightMultiple;
    private Double achievementsOrderMultiple;
    private Byte isGenOrderMultiple;
    private Long shopId;
    private Long platformShopId;
    private Integer buyerId;
    private Integer channelTalentId;
    private String nickname;
    private String authorOpenid;
    private String shortid;
    private Integer partnerId;
    private Integer partnerDeptId;
    private String partnerDeptIdPath;
    private String partnerDeptNamePath;
    private Integer channelId;
    private Integer channelDeptId;
    private String channelDeptIdPath;
    private String channelDeptNamePath;
    private Integer userCarrierId;
    private Double finalServiceRate;
    private Double finalServiceIncome;
    private Integer orderDataType;
    private Double talentEstimatedCommission;
    private Double talentRealCommission;
    private Double talentCommissionRate;
    private String uid;
    private String ext;

}
