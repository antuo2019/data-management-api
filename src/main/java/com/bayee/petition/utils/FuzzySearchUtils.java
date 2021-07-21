package com.bayee.petition.utils;

public class FuzzySearchUtils {

	 Integer sourceId; 					//来源

     Integer sourceSublevelId;			//来源子级

     Integer sourceSublevelTwoId;		//来源子子级

     Integer natureTypeId;				//性质类别

     Integer natureTypeSublevelId;		//性质类别子级

     Integer natureTypeSublevelTwoId;	//性质类别子子级
	 
	 Integer undertakeDeptId;			//承办单位
	 
	 Integer complainDeptId;			//投诉单位
	 
	 Integer duplicateFile;				//是否是重复件（0：是、1：否）

	 Integer concludeTheMatter;			//办结状态 （0：未办结、1：已办结）
	 
	 Integer verifiedSituation;  		//属实情况（0：属实、1：基本属实、2：不属实、3：其他）
	 
	 Integer stopComplaint;				//是否停访息诉（0：是、1：否）
	 
	 Integer dutyTypeId;				//追责内容

     Integer dutyTypeSublevelId;		//追责内容子级

     Integer dutyTypeSublevelTwoId;		//追责内容子子级
	 
	 Integer whetherOverdue;			//是否超期（0：是、1：否）
	 
	 Integer satisfaction;				//回访满意度（0：满意、1：基本满意、2：不满意、3：无需回访）
	 
	 String receptionTimeStartStr;		//接收日期（开始）如：2020-10-02
	 
	 String receptionTimeEndStr;		//接收日期（结束）如：2020-10-18
	 
	 String birthdayStartStr;			//投诉人出生日期（开始）如：1988-10-02
	 
	 String birthdayEndStr;				//投诉人出生日期（结束）如：2000-10-18
	 
	 Integer totalCountStart;			//回访信访历程总次数（开始）
	 
	 Integer totalCountEnd;				//回访信访历程总次数（结束）
	 
	 String content;					//搜索框内容（信访内容、信访发生地、信访人、身份证号、联系方式。。。）
	 
	 Integer pageNo;
	 
	 Integer pageSize;

	 
	public Integer getSourceId() {
		return sourceId;
	}

	public void setSourceId(Integer sourceId) {
		this.sourceId = sourceId;
	}

	public Integer getSourceSublevelId() {
		return sourceSublevelId;
	}

	public void setSourceSublevelId(Integer sourceSublevelId) {
		this.sourceSublevelId = sourceSublevelId;
	}

	public Integer getSourceSublevelTwoId() {
		return sourceSublevelTwoId;
	}

	public void setSourceSublevelTwoId(Integer sourceSublevelTwoId) {
		this.sourceSublevelTwoId = sourceSublevelTwoId;
	}

	public Integer getNatureTypeId() {
		return natureTypeId;
	}

	public void setNatureTypeId(Integer natureTypeId) {
		this.natureTypeId = natureTypeId;
	}

	public Integer getNatureTypeSublevelId() {
		return natureTypeSublevelId;
	}

	public void setNatureTypeSublevelId(Integer natureTypeSublevelId) {
		this.natureTypeSublevelId = natureTypeSublevelId;
	}

	public Integer getNatureTypeSublevelTwoId() {
		return natureTypeSublevelTwoId;
	}

	public void setNatureTypeSublevelTwoId(Integer natureTypeSublevelTwoId) {
		this.natureTypeSublevelTwoId = natureTypeSublevelTwoId;
	}

	public Integer getUndertakeDeptId() {
		return undertakeDeptId;
	}

	public void setUndertakeDeptId(Integer undertakeDeptId) {
		this.undertakeDeptId = undertakeDeptId;
	}

	public Integer getComplainDeptId() {
		return complainDeptId;
	}

	public void setComplainDeptId(Integer complainDeptId) {
		this.complainDeptId = complainDeptId;
	}

	public Integer getDuplicateFile() {
		return duplicateFile;
	}

	public void setDuplicateFile(Integer duplicateFile) {
		this.duplicateFile = duplicateFile;
	}

	public Integer getConcludeTheMatter() {
		return concludeTheMatter;
	}

	public void setConcludeTheMatter(Integer concludeTheMatter) {
		this.concludeTheMatter = concludeTheMatter;
	}

	public Integer getVerifiedSituation() {
		return verifiedSituation;
	}

	public void setVerifiedSituation(Integer verifiedSituation) {
		this.verifiedSituation = verifiedSituation;
	}

	public Integer getStopComplaint() {
		return stopComplaint;
	}

	public void setStopComplaint(Integer stopComplaint) {
		this.stopComplaint = stopComplaint;
	}

	public Integer getDutyTypeId() {
		return dutyTypeId;
	}

	public void setDutyTypeId(Integer dutyTypeId) {
		this.dutyTypeId = dutyTypeId;
	}

	public Integer getDutyTypeSublevelId() {
		return dutyTypeSublevelId;
	}

	public void setDutyTypeSublevelId(Integer dutyTypeSublevelId) {
		this.dutyTypeSublevelId = dutyTypeSublevelId;
	}

	public Integer getDutyTypeSublevelTwoId() {
		return dutyTypeSublevelTwoId;
	}

	public void setDutyTypeSublevelTwoId(Integer dutyTypeSublevelTwoId) {
		this.dutyTypeSublevelTwoId = dutyTypeSublevelTwoId;
	}

	public Integer getWhetherOverdue() {
		return whetherOverdue;
	}

	public void setWhetherOverdue(Integer whetherOverdue) {
		this.whetherOverdue = whetherOverdue;
	}

	public Integer getSatisfaction() {
		return satisfaction;
	}

	public void setSatisfaction(Integer satisfaction) {
		this.satisfaction = satisfaction;
	}

	public String getReceptionTimeStartStr() {
		return receptionTimeStartStr;
	}

	public void setReceptionTimeStartStr(String receptionTimeStartStr) {
		this.receptionTimeStartStr = receptionTimeStartStr;
	}

	public String getReceptionTimeEndStr() {
		return receptionTimeEndStr;
	}

	public void setReceptionTimeEndStr(String receptionTimeEndStr) {
		this.receptionTimeEndStr = receptionTimeEndStr;
	}

	public String getBirthdayStartStr() {
		return birthdayStartStr;
	}

	public void setBirthdayStartStr(String birthdayStartStr) {
		this.birthdayStartStr = birthdayStartStr;
	}

	public String getBirthdayEndStr() {
		return birthdayEndStr;
	}

	public void setBirthdayEndStr(String birthdayEndStr) {
		this.birthdayEndStr = birthdayEndStr;
	}

	public Integer getTotalCountStart() {
		return totalCountStart;
	}

	public void setTotalCountStart(Integer totalCountStart) {
		this.totalCountStart = totalCountStart;
	}

	public Integer getTotalCountEnd() {
		return totalCountEnd;
	}

	public void setTotalCountEnd(Integer totalCountEnd) {
		this.totalCountEnd = totalCountEnd;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	
	public Integer getPageNo() {
		if(this.pageSize!=null) {
			return (pageNo-1)*pageSize;
		}
		return pageNo;
	}

	public void setPageNo(Integer pageNo) {
		this.pageNo = pageNo;
	}

	public Integer getPageSize() {
		return pageSize;
	}

	public void setPageSize(Integer pageSize) {
		this.pageSize = pageSize;
	}

	
	
}
