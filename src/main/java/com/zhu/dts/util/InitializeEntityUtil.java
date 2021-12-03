package com.zhu.dts.util;

import com.zhu.dts.entity.ParameterEntity;

/**
 * @Author ZhuHaiBo
 * @Create 2021/11/30 1:44
 */
public class InitializeEntityUtil {

    private volatile static ParameterEntity parameterEntity = new ParameterEntity();

    private InitializeEntityUtil() {
    }

    public static ParameterEntity getInstance() {
        if (null == parameterEntity) {
            parameterEntity = new ParameterEntity();
        }
        return parameterEntity;
    }
}
