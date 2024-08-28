/*------------------------------------------------------------------------------
 *******************************************************************************
 * COPYRIGHT Ericsson 2016
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *******************************************************************************
 *----------------------------------------------------------------------------*/
package com.ericsson.component.aia.services.bps.flink.test.app;

import java.io.Serializable;

/**
 * POJO used by the test classes.
 */
public class PositionEvent implements Serializable {

    String vin;

    double latitidue;

    double longitude;

    double bearing;

    long carPositionTime;

    /**
     * @return the bearing
     */
    public double getBearing() {
        return bearing;
    }

    /**
     * @return the carPositionTime
     */
    public long getCarPositionTime() {
        return carPositionTime;
    }

    /**
     * @return the latitidue
     */
    public double getLatitidue() {
        return latitidue;
    }

    /**
     * @return the longitude
     */
    public double getLongitude() {
        return longitude;
    }

    /**
     * @return the vin
     */
    public String getVin() {
        return vin;
    }

    /**
     * @param bearing
     *            the bearing to set
     */
    public void setBearing(final double bearing) {
        this.bearing = bearing;
    }

    /**
     * @param carPositionTime
     *            the carPositionTime to set
     */
    public void setCarPositionTime(final long carPositionTime) {
        this.carPositionTime = carPositionTime;
    }

    /**
     * @param latitidue
     *            the latitidue to set
     */
    public void setLatitidue(final double latitidue) {
        this.latitidue = latitidue;
    }

    /**
     * @param longitude
     *            the longitude to set
     */
    public void setLongitude(final double longitude) {
        this.longitude = longitude;
    }

    /**
     * @param vin
     *            the vin to set
     */
    public void setVin(final String vin) {
        this.vin = vin;
    }

}
