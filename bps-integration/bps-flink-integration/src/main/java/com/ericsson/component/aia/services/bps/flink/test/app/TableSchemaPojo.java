package com.ericsson.component.aia.services.bps.flink.test.app;

/*------------------------------------------------------------------------------
 *******************************************************************************
 * COPYRIGHT Ericsson 2017
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *******************************************************************************
 *----------------------------------------------------------------------------*/

/**
 * Schema Pojo used to create output table in jdbc data base.
 */
@SuppressWarnings({ "PMD.ShortVariable", "PMD.SuspiciousConstantFieldName" })
public class TableSchemaPojo {

    public String ID;

    public String TRANSACTION_DATE;

    public String PRODUCT;

    public String PRICE;

    public String PAYMENT_TYPE;

    public String NAME;

    public String CITY;

    public String STATE;

    public String COUNTRY;

    public String ACCOUNT_CREATED;

    public String LAST_LOGIN;

    public String LATITUDE;

    public String LONGITUDE;

    public String getID() {
        return ID;
    }

    public void setID(final String ID) {
        this.ID = ID;
    }

    public String getTRANSACTION_DATE() {
        return TRANSACTION_DATE;
    }

    public void setTRANSACTION_DATE(final String TRANSACTION_DATE) {
        this.TRANSACTION_DATE = TRANSACTION_DATE;
    }

    public String getPRODUCT() {
        return PRODUCT;
    }

    public void setPRODUCT(final String PRODUCT) {
        this.PRODUCT = PRODUCT;
    }

    public String getPRICE() {
        return PRICE;
    }

    public void setPRICE(final String PRICE) {
        this.PRICE = PRICE;
    }

    public String getPAYMENT_TYPE() {
        return PAYMENT_TYPE;
    }

    public void setPAYMENT_TYPE(final String PAYMENT_TYPE) {
        this.PAYMENT_TYPE = PAYMENT_TYPE;
    }

    public String getNAME() {
        return NAME;
    }

    public void setNAME(final String NAME) {
        this.NAME = NAME;
    }

    public String getCITY() {
        return CITY;
    }

    public void setCITY(final String CITY) {
        this.CITY = CITY;
    }

    public String getSTATE() {
        return STATE;
    }

    public void setSTATE(final String STATE) {
        this.STATE = STATE;
    }

    public String getCOUNTRY() {
        return COUNTRY;
    }

    public void setCOUNTRY(final String COUNTRY) {
        this.COUNTRY = COUNTRY;
    }

    public String getACCOUNT_CREATED() {
        return ACCOUNT_CREATED;
    }

    public void setACCOUNT_CREATED(final String ACCOUNT_CREATED) {
        this.ACCOUNT_CREATED = ACCOUNT_CREATED;
    }

    public String getLAST_LOGIN() {
        return LAST_LOGIN;
    }

    public void setLAST_LOGIN(final String LAST_LOGIN) {
        this.LAST_LOGIN = LAST_LOGIN;
    }

    public String getLATITUDE() {
        return LATITUDE;
    }

    public void setLATITUDE(final String LATITUDE) {
        this.LATITUDE = LATITUDE;
    }

    public String getLONGITUDE() {
        return LONGITUDE;
    }

    public void setLONGITUDE(final String LONGITUDE) {
        this.LONGITUDE = LONGITUDE;
    }

}
