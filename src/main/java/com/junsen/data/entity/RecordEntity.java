package com.junsen.data.entity;

public class RecordEntity {

    private String patentId;
    private String ansName;
    private String apno;
    private Integer pbdt;
    private String patentType;

    public RecordEntity(String patentId,String ansName,String apno,Integer pbdt,String patentType){
        this.ansName=ansName;
        this.patentId=patentId;
        this.apno=apno;
        this.pbdt=pbdt;
        this.patentType=patentType;
    }
    public String getPatentId() {
        return patentId;
    }

    public void setPatentId(String patentId) {
        this.patentId = patentId;
    }

    public String getAnsName() {
        return ansName;
    }

    public void setAnsName(String ansName) {
        this.ansName = ansName;
    }

    public String getApno() {
        return apno;
    }

    public void setApno(String apno) {
        this.apno = apno;
    }

    public Integer getPbdt() {
        return pbdt;
    }

    public void setPbdt(Integer pbdt) {
        this.pbdt = pbdt;
    }

    public String getPatentType() {
        return patentType;
    }

    public void setPatentType(String patentType) {
        this.patentType = patentType;
    }


}
