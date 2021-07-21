package com.bayee.petition.service.impl;

import com.bayee.petition.domain.HbCatlogDAO;
import com.bayee.petition.mapper.HbCatlogMapper;
import com.bayee.petition.service.HbCatlogService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @Description: TODO
 * @Author YueCang
 * @Date 2021/1/2711:16
 */
@Service
public class HbCatlogServiceImpl implements HbCatlogService {

    @Autowired
    HbCatlogMapper hbCatlogMapper;

    @Override
    public List<HbCatlogDAO> getCurrentJob() {
        return hbCatlogMapper.getCurrentJob();
    }

    @Override
    public List<HbCatlogDAO> getDataCollectionTrend() {
        return hbCatlogMapper.getDataCollectionTrend();
    }

    @Override
    public List<HbCatlogDAO> getDataTop5() {
        return hbCatlogMapper.getDataTop5();
    }

    @Override
    public List<HbCatlogDAO> getDataCategory() {
        return hbCatlogMapper.getDataCategory();
    }

    @Override
    public List<HbCatlogDAO> getDataSources() {
        return hbCatlogMapper.getDataSources();
    }

    @Override
    public int count(HbCatlogDAO hbCatlogDAO) {
        return hbCatlogMapper.count(hbCatlogDAO);
    }

    @Override
    public List<HbCatlogDAO> findAllNature(HbCatlogDAO hbCatlogDAO) {
        return hbCatlogMapper.findAllNature(hbCatlogDAO);
    }

	@Override
	public List<String> selectCategory() {
		// TODO Auto-generated method stub
		return hbCatlogMapper.selectCategory();
	}

	@Override
	public List<String> selectOrigin() {
		// TODO Auto-generated method stub
		return hbCatlogMapper.selectOrigin();
	}

	@Override
	public HbCatlogDAO findById(Integer id) {
		// TODO Auto-generated method stub
		return hbCatlogMapper.findById(id);
	}

}
