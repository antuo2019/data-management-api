package com.bayee.petition.service.impl;

import com.bayee.petition.mapper.DataInfoMapper;
import com.bayee.petition.service.DataInfoService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @Description: TODO
 * @Author YueCang
 * @Date 2021/1/2711:14
 */
@Service
public class DataInfoServiceImpl implements DataInfoService {

    @Autowired
    DataInfoMapper dataInfoMapper;

    @Override
    public int getDataCount() {
        return dataInfoMapper.getDataCount();
    }

    @Override
    public int getCategoryCount() {
        return dataInfoMapper.getCategoryCount();
    }

    @Override
    public int getTableCount() {
        return dataInfoMapper.getTableCount();
    }
}
