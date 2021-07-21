package com.bayee.petition.utils;

import java.util.List;


public class Pagetion {

	private List list;// 需要分页查询显示的表

	private int pageNo;// 当前页码    必传

	private int pageSize;// 每页显示的数量   必传

	private int totalCount;// 总记录数   必传

	private int totalPage;// 总页码 

	private int begin;  //

	public List getList() {
		return list;
	}

	public void setList(List list) {
		this.list = list;
	}

	/**
	 * 得到当前页码
	 * 
	 * @return
	 */
	public int getPageNo() {
		// 为了页面的有效性，进行最大页和最小页的判断
		if (pageNo > this.getTotalPage()) {
			// 如果当前页码大于了总页码数则等于页码大小等于总记录数
			pageNo = this.getTotalPage();
		}

		if (pageNo < 1) {
			// 如果当前页码小于了1数则等于页码大小等于1
			pageNo = 1;
		}
		return pageNo;
	}

	/**
	 * 设置当前页码
	 * 
	 * @param pageNo
	 */
	public void setPageNo(int pageNo) {
		this.pageNo = pageNo;
	}

	/**
	 * 得到每页显示的数量
	 * 
	 * @return
	 */
	public int getPageSize() {
		return pageSize;
	}

	/**
	 * 设置每页显示的数量
	 * 
	 * @param pageSize
	 */
	public void setPageSize(int pageSize) {
		this.pageSize = pageSize;
	}

	/**
	 * 得到总记录数
	 * 
	 * @return
	 */
	public int getTotalCount() {
		return totalCount;
	}

	/**
	 * 设置总记录数
	 * 
	 * @param totalCount
	 */
	public void setTotalCount(int totalCount) {
		this.totalCount = totalCount;
	}

	/**
	 * 得到总页码数
	 * 
	 * @return
	 */
	public int getTotalPage() {
		// 页面的数量计算
		// 总页码数 = 总记录数 % 每页显示的数量长度 如果等于0
		// 则总页码数 = 总记录数 / 每页显示的数量长度
		// 否则 = 总记录数 / 每页显示的数量长度 + 1
		//this.totalPage = totalCount % pageSize == 0 ? totalCount / pageSize : totalCount / pageSize + 1;
		return this.totalPage;
	}

	public Pagetion(int pageNo, int pageSize) {
		super();
		this.pageNo = pageNo;
		this.pageSize = pageSize;
	}

	public Pagetion() {
		super();
		// TODO Auto-generated constructor stub
	}

	public int getBegin() {
		this.begin = (this.getPageNo()-1)*this.getPageSize();
		return this.begin;
	}	
	
}
