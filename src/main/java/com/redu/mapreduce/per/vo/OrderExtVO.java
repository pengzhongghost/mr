package com.redu.mapreduce.per.vo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author pengzhong
 * @since 2023/8/9
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderExtVO {

    private String hiPartnerid;

    private String hiChannelid;

    private String partnerGroupId;

    private String channelGroupId;

    private String partnerFinalServiceIncome;

    private String channelFinalServiceIncome;

    private String partnerWeithtServiceIncome;

}
