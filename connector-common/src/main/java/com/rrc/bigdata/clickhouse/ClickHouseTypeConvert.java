package com.rrc.bigdata.clickhouse;

import org.apache.commons.lang3.StringUtils;

import static com.rrc.bigdata.constant.ClickHouseTypeConstant.*;

public class ClickHouseTypeConvert {

    /**
     * 判断输入类型是否是 ClickHouse 中的数值类型
     *
     * @param type 输入类型
     * @return 是数值类型返回 true, 否则返回 false
     */
    public static boolean isNumberType(String type) {
        if (StringUtils.isEmpty(type)) {
            return false;
        }
        if (type.startsWith(DECIMAL)) {
            return true;
        }
        switch (type) {
            case INT8:
            case INT16:
            case INT32:
            case INT64:
            case UINT8:
            case UINT16:
            case UINT32:
            case UINT64:
            case FLOAT32:
            case FLOAT64:
                return true;
            default:
                return false;
        }
    }

}
