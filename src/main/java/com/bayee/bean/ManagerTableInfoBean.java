package com.bayee.bean;

import java.io.Serializable;

/**
 * @Description: 表结构描述
 * @Author YueCang
 * @Date 2021/1/1910:07
 */
public class ManagerTableInfoBean implements Serializable {

    private int id;
    private String tableName;
    private String tableNameCh;
    private String category;
    private String originSystem;
    private int injectionType;
    private String updateFrequency;
    private int securityLeve;
    private String tags;
    private String fields;
    private String fieldsCh;
    private String creationDate;
    private int count;

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
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

    public int getInjectionType() {
        return injectionType;
    }

    public void setInjectionType(int injectionType) {
        this.injectionType = injectionType;
    }

    public String getUpdateFrequency() {
        return updateFrequency;
    }

    public void setUpdateFrequency(String updateFrequency) {
        this.updateFrequency = updateFrequency;
    }

    public int getSecurityLeve() {
        return securityLeve;
    }

    public void setSecurityLeve(int securityLeve) {
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

    public String getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(String creationDate) {
        this.creationDate = creationDate;
    }

    @Override
    public String toString() {
        return "ManagerTableInfoBean{" +
                "id=" + id +
                ", tableName='" + tableName + '\'' +
                ", tableNameCh='" + tableNameCh + '\'' +
                ", category='" + category + '\'' +
                ", originSystem='" + originSystem + '\'' +
                ", injectionType=" + injectionType +
                ", updateFrequency='" + updateFrequency + '\'' +
                ", securityLeve=" + securityLeve +
                ", tags='" + tags + '\'' +
                ", fields='" + fields + '\'' +
                ", fieldsCh='" + fieldsCh + '\'' +
                ", creationDate='" + creationDate + '\'' +
                ", count=" + count +
                '}';
    }

    public ManagerTableInfoBean(Builder builder){
        this.id = builder.id;
        this.tableName = builder.tableName;
        this.tableNameCh = builder.tableNameCh;
        this.category = builder.category;
        this.originSystem = builder.originSystem;
        this.injectionType = builder.injectionType;
        this.updateFrequency = builder.updateFrequency;
        this.securityLeve = builder.securityLeve;
        this.tags = builder.tags;
        this.fields = builder.fields;
        this.fieldsCh = builder.fieldsCh;
        this.creationDate = builder.creationDate;
        this.count = builder.count;
    }

    public static class Builder{
        private int id;
        private String tableName;
        private String tableNameCh;
        private String category;
        private String originSystem;
        private int injectionType;
        private String updateFrequency;
        private int securityLeve;
        private String tags;
        private String fields;
        private String fieldsCh;
        private String creationDate;
        private int count;

        public Builder id(int id){
            this.id = id;
            return this;
        }

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder tableNameCh(String tableNameCh) {
            this.tableNameCh = tableNameCh;
            return this;
        }

        public Builder category(String category) {
            this.category = category;
            return this;
        }

        public Builder originSystem(String originSystem) {
            this.originSystem = originSystem;
            return this;
        }

        public Builder injectionType(int injectionType) {
            this.injectionType = injectionType;
            return this;
        }

        public Builder updateFrequency(String updateFrequency) {
            this.updateFrequency = updateFrequency;
            return this;
        }

        public Builder securityLeve(int securityLeve) {
            this.securityLeve = securityLeve;
            return this;
        }

        public Builder tags(String tags) {
            this.tags = tags;
            return this;
        }

        public Builder fields(String fields) {
            this.fields = fields;
            return this;
        }

        public Builder fieldsCh(String fieldsCh) {
            this.fieldsCh = fieldsCh;
            return this;
        }

        public Builder creationDate(String creationDate) {
            this.creationDate = creationDate;
            return this;
        }

        public Builder count(int count) {
            this.count = count;
            return this;
        }

        public ManagerTableInfoBean build(){
            return new ManagerTableInfoBean(this);
        }
    }
}
