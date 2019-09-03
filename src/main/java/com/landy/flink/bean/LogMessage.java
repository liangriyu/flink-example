package com.landy.flink.bean;

import com.landy.flink.utils.DateUtil;
import java.text.SimpleDateFormat;
import java.util.Date;

public class LogMessage {

    /**
     * 用户Id
     */
    private Long uid;

    /**
     * 用户账号
     */
    private String account;

    /**
     * 接口地址
     */
    private String url;

    /**
     * 接口方法
     */
    private String method;

    /**
     * IP地址
     */
    private String ip;

    /**
     * 接口入参
     */
    private String paramBody;

    /**
     * 日志记录时间
     */
    private Date dateTime;

    /**
     * 日志记录时间（毫秒）
     */
    private Long ms;

    /**
     * 企业ID
     */
    private Long entityId;

    /**
     * 企业名称
     */
    private String entityName;

    /**
     * 用户名
     */
    private String userName;

    public Long getUid() {
        return uid;
    }

    public void setUid(Long uid) {
        this.uid = uid;
    }

    public String getAccount() {
        return account;
    }

    public void setAccount(String account) {
        this.account = account;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getParamBody() {
        return paramBody;
    }

    public void setParamBody(String paramBody) {
        this.paramBody = paramBody;
    }

    public Date getDateTime() {
        return dateTime;
    }

    public void setDateTime(Date dateTime) {
        this.dateTime = dateTime;
    }

    public Long getMs() {
        return ms;
    }

    public void setMs(Long ms) {
        this.ms = ms;
    }

    public Long getEntityId() {
        return entityId;
    }

    public void setEntityId(Long entityId) {
        this.entityId = entityId;
    }

    public String getEntityName() {
        return entityName;
    }

    public void setEntityName(String entityName) {
        this.entityName = entityName;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    @Override
    public String toString() {
        SimpleDateFormat sdf = new SimpleDateFormat(DateUtil.DATE_FORMAT);
        return "LogMessage{" +
                "uid=" + uid +
                ", account='" + account + '\'' +
                ", url='" + url + '\'' +
                ", method='" + method + '\'' +
                ", ip='" + ip + '\'' +
                ", paramBody='" + paramBody + '\'' +
                ", dateTime=" + sdf.format(dateTime) +
                ", ms=" + ms +
                ", entityId=" + entityId +
                ", entityName='" + entityName + '\'' +
                ", userName='" + userName + '\'' +
                '}';
    }
}
