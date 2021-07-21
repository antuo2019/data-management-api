package com.bayee.petition.domain;

import com.bayee.petition.utils.DateUtils;

import java.util.Date;

/**
 * @Description: TODO
 * @Author YueCang
 * @Date 2021/1/279:49
 */
public class HbCatlogDAO {
	private Integer id;
    private String tableName;
    private String tableNameCh;
    private String category;
    private String originSystem;
    private String injectionType;
    private String updateFrequency;
    private String securityLeve;
    private String tags;
    private String fields;
    private String fieldsCh;
    private Date creationDate;
    private int count;
    private String currentJobDate;
    private String dateToDay;

    public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	//总数
    private Integer counts; //
    //当前页码
    private Integer page;
    //当前页码显示数量
    private Integer pageSize;

    public Integer getCounts() {
        return counts;
    }

    public void setCounts(Integer counts) {
        this.counts = counts;
    }

    public Integer getPage() {
        return page;
    }

    public void setPage(Integer page) {
        this.page = page;
    }

    public Integer getPageSize() {
        return pageSize;
    }

    public void setPageSize(Integer pageSize) {
        this.pageSize = pageSize;
    }

    public String getCurrentJobDate() {
        if(creationDate!=null){
            return DateUtils.dateToString(this.creationDate);
        }
        return "";
    }

    public void setCurrentJobDate(String currentJobDate) {
        this.currentJobDate = currentJobDate;
    }

    public String getDateToDay() {
        if(creationDate != null) return DateUtils.dateToDayString(this.creationDate);
        return "";
    }

    public void setDateToDay(String dateToDay) {
        this.dateToDay = dateToDay;
    }

    public HbCatlogDAO() {}

    public HbCatlogDAO(String tableName, String tableNameCh,
                       String category, String originSystem,
                       String injectionType, String updateFrequency,
                       String securityLeve, String tags,
                       String fields, String fieldsCh,
                       Date creationDate, int count) {
        this.tableName = tableName;
        this.tableNameCh = tableNameCh;
        this.category = category;
        this.originSystem = originSystem;
        this.injectionType = injectionType;
        this.updateFrequency = updateFrequency;
        this.securityLeve = securityLeve;
        this.tags = tags;
        this.fields = fields;
        this.fieldsCh = fieldsCh;
        this.creationDate = creationDate;
        this.count = count;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getTableNameCh() {
        return tableNameCh;
    }

    public void setTableNameCh(String tableNameCh) {
        this.tableNameCh = tableNameCh;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getOriginSystem() {
        return originSystem;
    }

    public void setOriginSystem(String originSystem) {
        this.originSystem = originSystem;
    }

    public String getInjectionType() {
        return injectionType;
    }

    public void setInjectionType(String injectionType) {
        this.injectionType = injectionType;
    }

    public String getUpdateFrequency() {
        return updateFrequency;
    }

    public void setUpdateFrequency(String updateFrequency) {
        this.updateFrequency = updateFrequency;
    }

    public String getSecurityLeve() {
        return securityLeve;
    }

    public void setSecurityLeve(String securityLeve) {
        this.securityLeve = securityLeve;
    }

    public String getTags() {
        return tags;
    }

    public void setTags(String tags) {
        this.tags = tags;
    }

    public String getFields() {
        return fields;
    }

    public void setFields(String fields) {
        this.fields = fields;
    }

    public String getFieldsCh() {
        return fieldsCh;
    }

    public void setFieldsCh(String fieldsCh) {
        this.fieldsCh = fieldsCh;
    }

    public Date getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(Date creationDate) {
        this.creationDate = creationDate;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "HbCatlogDAO{" +
                "tableName='" + tableName + '\'' +
                ", tableNameCh='" + tableNameCh + '\'' +
                ", category='" + category + '\'' +
                ", originSystem='" + originSystem + '\'' +
                ", injectionType='" + injectionType + '\'' +
                ", updateFrequency='" + updateFrequency + '\'' +
                ", securityLeve='" + securityLeve + '\'' +
                ", tags='" + tags + '\'' +
                ", fields='" + fields + '\'' +
                ", fieldsCh='" + fieldsCh + '\'' +
                ", creationDate=" + creationDate +
                ", count=" + count +
                '}';
    }
}
