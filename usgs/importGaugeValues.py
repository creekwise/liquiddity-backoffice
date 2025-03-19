#!/bin/python3

###################################################################################################
# Description goes here
###################################################################################################

import sys
import configparser
import argparse
from datetime import datetime, timezone, timedelta

import requests
import xml.etree.ElementTree as ET

import ConfigUtil
import PostgresUtil
import JobUtil
import LiquiddityModel
from LiquiddityModel import State, Site, Gauge, GaugeCheckin

################################################
# createJobArgParser
################################################

def createJobArgParser():
    result = argparse.ArgumentParser()

    result.add_argument('--props', help='Properties file path', required=True)
    result.add_argument('-l', '--logDir', help='Log file', required=True)
    result.add_argument('-s', '--state', help='State', required=False)

    return result
################################################
# importGauge
################################################

def importGauge(argConn, argGauge):

    siteUrlTempl = "https://waterservices.usgs.gov/nwis/iv/?site={}&parameterCd={}&startDT={}"

    lastUsgsTS = argGauge.lastUsgsTS
    startDate = lastUsgsTS + timedelta(seconds=1)
    startDateStr = startDate.strftime("%Y-%m-%dT%H:%M:%S")

    gaugeType = argGauge.gaugeType
    siteId = argGauge.siteId
    siteUrl = siteUrlTempl.format(siteId, gaugeType, startDateStr)

    site = Site.find(argConn, siteId)

    siteName = site.siteName
    gaugeId = "{}:{}".format(siteId, gaugeType)
    gaugeDisplay = "{}: ({})".format(siteName, gaugeId)
    print(gaugeDisplay)
    print("\t" + siteUrl)



    lastCheckinVal = None
    lastUsgsTS = None
    lastQualifs = None

    numCheckins = 0

    try:

        response = requests.get(siteUrl)
        respText = response.content.decode("utf-8");
        respRoot = ET.fromstring(respText)

        seriesElem = respRoot.find("{http://www.cuahsi.org/waterML/1.1/}timeSeries")
        valuesElem = seriesElem.find("{http://www.cuahsi.org/waterML/1.1/}values")
        valElems = valuesElem.findall("{http://www.cuahsi.org/waterML/1.1/}value")

        for valElem in valElems:
            numCheckins += 1
            gaugeValStr = valElem.text
            checkinVal = float(gaugeValStr)

            gaugeTimestampStr = valElem.attrib['dateTime']
            qualifs = valElem.attrib['qualifiers']

            gaugeTS = datetime.fromisoformat(gaugeTimestampStr)

            gaugeCheckin = GaugeCheckin(argGauge.gaugeId, gaugeTS, checkinVal, qualifs)
            gaugeCheckin.persist(argConn)

            print("\t{} - {} - {}".format(gaugeTS, checkinVal, qualifs))

            lastCheckinVal = checkinVal
            lastUsgsTS = gaugeTS
            lastQualifs = qualifs
    except:
        print("\t<Unable to fetch gauge data for: {}>".format(gaugeDisplay))

    if numCheckins > 0:
        argGauge.lastVal = lastCheckinVal
        argGauge.lastUsgsTS = lastUsgsTS
        argGauge.qualifs = lastQualifs

        argGauge.update(argConn)

    return numCheckins
################################################
# getPayload
################################################

def getPayload(argConn, argStatePostal):

    result = []

    #stateClause = " AND s.state_cd = '{}'".format(argStatePostal) if argStatePostal else ""

    if argStatePostal:
        statesByPostal = LiquiddityModel.getStates(argConn)
        state = statesByPostal[argStatePostal]
        stateCd = state.stateCd
        stateClause = " AND s.state_cd = '{}'".format(stateCd)
    else:
        stateClause = ""

    try:
        cur = argConn.cursor()

        sql = "SELECT g.gauge_id, g.site_id, g.gauge_type, g.last_usgs_ts, g.qualifs, g.active_status, g.last_val \
                FROM td_gauge g JOIN td_site s ON g.site_id = s.site_id WHERE do_import = TRUE{} \
                ORDER BY g.site_id, g.gauge_type;".format(stateClause)

        cur.execute(sql)
        rows = cur.fetchall()

        for row in rows:
            gaugeId = row[0]
            siteId = row[1]
            gaugeType = row[2]
            lastUsgsTS = row[3]
            qualifs = row[4]
            activeStatus = row[5]
            lastVal = row[6]


            gauge = Gauge(gaugeId, siteId, gaugeType, qualifs, activeStatus, lastVal, lastUsgsTS)

            result.append(gauge)

    finally:
        cur.close()

    return result;


################################################
# main
################################################

if __name__ == '__main__':

    # print("in importGaugeValues")

    argparser = createJobArgParser()

    args = argparser.parse_args()

    propFile = args.props
    rqstStateCode = args.state

    config = configparser.ConfigParser()
    config.read(propFile)

    fileSysConfHash = {}
    fileSysConfHash['logDir'] = args.logDir

    fileSysConf = ConfigUtil.getFileSysConfig(fileSysConfHash)

    jobConfig = JobUtil.initJob("GAUGE VALUE IMPORT", "manual", fileSysConf)
    jobLogger = jobConfig.logger

    dbConnConf = ConfigUtil.getPostgresConfig(config['LIQUIDDITY-CONN'])

    cxn = PostgresUtil.getConnection(dbConnConf, jobLogger)

    payload = getPayload(cxn, rqstStateCode)

    for gauge in payload:
        importGauge(cxn, gauge)

    JobUtil.endJob(jobConfig)
