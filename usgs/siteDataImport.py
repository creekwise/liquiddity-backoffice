#!/bin/python3

###################################################################################################
# Description goes here
###################################################################################################

import sys
import configparser
import argparse
from datetime import datetime, timezone

import requests
import xml.etree.ElementTree as ET

import ConfigUtil
import PostgresUtil
import JobUtil
from LiquiddityModel import State, County, Site, Gauge


################################################
# createJobArgParser
################################################

def createJobArgParser():
    result = argparse.ArgumentParser()

    result.add_argument('--props', help='Properties file path', required=True)
    result.add_argument('-l', '--logDir', help='Log file', required=True)
    result.add_argument('-s', '--state', help='State', required=False)
    result.add_argument('-v', '--verify', help='Verify gauge status', action="store_true")
    result.add_argument('-r', '--refresh', help='Refresh site data', action="store_true")

    return result

################################################
# purgeStateSites
################################################

def purgeStateSites(argDbCxn, argStateCd):

    template = "DELETE FROM td_site WHERE state_cd = %s;"

    insVals = [ argStateCd ]

    cmdResult = PostgresUtil.executeStatement(argDbCxn, template, insVals)

    if cmdResult.success:
        return True
    else:
        return False

################################################
# purgeSiteGauges
################################################

def purgeSiteGauges(argDbCxn, argSiteNo):

    template = "DELETE FROM tr_gauge WHERE site_no = %(siteNo)s;"

    insVals = { "siteNo" : argSiteNo }

    cmdResult = PostgresUtil.executeStatement(argDbCxn, template, insVals)

    if cmdResult.success:
        return True
    else:
        return False

################################################
# verifyState
################################################

def verifyState(argConn, argSiteNo):
    stateSites = getStateSites(argConn, argSiteNo)

    stateSiteNum = len(stateSites)

    siteCntr = 0

    for stateSiteNo in stateSites.keys():
        siteCntr += 1
        stateSite = stateSites[stateSiteNo]
        siteGauges = verifySite(argConn, stateSite)
        print("({}/{}) Site: {} Gauges: {}".format(siteCntr, stateSiteNum, stateSiteNo, siteGauges))

################################################
# verifySite
################################################

def verifySite(argConn, argSite):

    argSiteNo = argSite.siteId

    purgeSiteGauges(argConn, argSiteNo)

    siteUrlTempl = "https://waterservices.usgs.gov/nwis/iv/?site={}"
    siteUrl = siteUrlTempl.format(argSiteNo)

    response = requests.get(siteUrl)
    respText = response.content.decode("utf-8");

    respRoot = ET.fromstring(respText)

    gaugeElems = respRoot.findall("{http://www.cuahsi.org/waterML/1.1/}timeSeries")

    gaugeCount = len(gaugeElems)

    siteIsActive = False
    #siteLastUsgsTS = argSite.lastUsgsTS
    procTS = datetime.now(timezone.utc)

    if gaugeCount > 0:

        #print("Site: {} Gauges: {}".format(argSiteNo, gaugeCount))

        for gaugeElem in gaugeElems:
            gaugeUsgsId = gaugeElem.attrib['name']
            gaugeIdElems = gaugeUsgsId.split(':')
            gaugeType = gaugeIdElems[2]

            gaugeId = "{}:{}".format(argSiteNo, gaugeType)

            valsGroupElems = gaugeElem.findall("{http://www.cuahsi.org/waterML/1.1/}values")
            valsGroupCount = len(valsGroupElems)

            for valsGroupElem in valsGroupElems:
                valElems = valsGroupElem.findall("{http://www.cuahsi.org/waterML/1.1/}value")
                valCount = len(valElems)

                #print("\t{} val groups: {} vals {}: ".format(gaugeType, valsGroupCount, valCount))

                for valElem in valElems:
                    gaugeValStr = valElem.text
                    gaugeVal = float(gaugeValStr)
                    gaugeTimestampStr = valElem.attrib['dateTime']

                    gaugeTimestamp = datetime.fromisoformat(gaugeTimestampStr)

                    qualifs = valElem.attrib['qualifiers']

                    daysOld = abs((procTS - gaugeTimestamp).days)

                    if daysOld < 3:
                        gaugeStatus = 'A'

                        if not argSite.countyCd:
                            sourceInfoElem = gaugeElem.find("{http://www.cuahsi.org/waterML/1.1/}sourceInfo")
                            sitePropElems = sourceInfoElem.findall("{http://www.cuahsi.org/waterML/1.1/}siteProperty")

                            locAttribs = {}

                            for sitePropElem in sitePropElems:
                                propName = sitePropElem.attrib['name']
                                propVal = sitePropElem.text
                                locAttribs[propName] = propVal

                            argSite.countyCd = locAttribs['countyCd']
                    else:
                        gaugeStatus = 'X'

                    if not siteIsActive and gaugeStatus == 'A': siteIsActive = True

                    gauge = Gauge(gaugeId, argSiteNo, gaugeType, qualifs, gaugeStatus, gaugeVal, gaugeTimestamp)
                    gauge.persist(argConn)

                    #print("\t\t gauge val: {} time: {} qualifiers {}".format(gaugeValStr, gaugeTimestampStr, qualifs))

    argSite.activeStatus = 'A' if siteIsActive else 'X'

    argSite.update(argConn)

    return gaugeCount

################################################
# getStateSites
################################################

def getStateSites(argConn, argStateCd):

    result = {}

    try:
        cur = argConn.cursor()

        sql = "SELECT site_id, site_name, state_cd, coord_lat, coord_long, hydro_unit_cd, \
            active_status FROM td_site WHERE state_cd = '{}';".format(argStateCd)

        cur.execute(sql)
        rows = cur.fetchall()

        for row in rows:

            siteId = row[0]
            siteName = row[1]
            stateCd = row[2]
            coordLat = row[3]
            coordLong = row[4]
            hydroUnitCd = row[5]
            activeStatus = row[6]

            site = Site(siteId, siteName, stateCd, coordLat, coordLong, hydroUnitCd, activeStatus)

            result[siteId] = site

    finally:
        cur.close()

    return result;



################################################
# getStates
################################################

def getStates(argConn):

    result = {}

    try:
        cur = argConn.cursor()

        sql = "SELECT state_cd, state_postal, state_name FROM tr_state ORDER BY state_cd;"

        cur.execute(sql)
        rows = cur.fetchall()

        for row in rows:
            stateCd = row[0]
            statePostal = row[1]
            stateName = row[2]

            state = State(stateCd, statePostal, stateName)

            result[statePostal] = state

    finally:
        cur.close()

    return result;

################################################
# importStateSites
################################################

def importStateSites(argConn, stateCd):

    purgeStateSites(argConn, stateCd)
    stateUrlTempl = "https://waterservices.usgs.gov/nwis/site/?stateCd={}&siteType=ST"

    stateURL = stateUrlTempl.format(stateCd)
    print("Site List URL for state of {}: {}".format(stateCd, stateURL))

    response = requests.get(stateURL)
    respText = response.text;

    respLines = respText.splitlines();

    contentLineNo = 0

    for respLine in respLines:

        if respLine.startswith('#'): continue

        contentLineNo += 1;

        if contentLineNo > 2:
            # print(respLine)

            lineCols = respLine.split("\t")
            # agency_cd
            siteId = lineCols[1]
            siteName = lineCols[2]
            # site_tp_cd
            coordLatStr = lineCols[4]
            coordLongStr = lineCols[5]
            hydroUnitCd = lineCols[11]

            coordLat = float(coordLatStr) if coordLatStr else None
            coordLong = float(coordLongStr) if coordLongStr else None

            outLine = "{} - {} - {} - {} - {} - {}".format(stateCd, siteId, siteName, coordLat, coordLong,
                                                           hydroUnitCd)

            newSite = Site(siteId, siteName, stateCd, coordLat, coordLong, hydroUnitCd)
            newSite.persist(argConn)

            print(outLine)


################################################
# main
################################################

if __name__ == '__main__':

    print("in siteDataImport")

    argparser = createJobArgParser()

    args = argparser.parse_args()

    propFile = args.props
    rqstStatePostal = args.state
    verifyGauges = args.verify
    importSites = args.refresh

    config = configparser.ConfigParser()
    config.read(propFile)

    fileSysConfHash = {}
    fileSysConfHash['logDir'] = args.logDir

    fileSysConf = ConfigUtil.getFileSysConfig(fileSysConfHash)

    jobConfig = JobUtil.initJob("SITE IMPORT", "manual", fileSysConf)
    jobLogger = jobConfig.logger

    dbConnConf = ConfigUtil.getPostgresConfig(config['LIQUIDDITY-CONN'])

    cxn = PostgresUtil.getConnection(dbConnConf, jobLogger)

    statesByPostal = getStates(cxn)

    if importSites:

        if rqstStatePostal:
            state = statesByPostal[rqstStatePostal]
            rqstStateCd = state.stateCd
            importStateSites(cxn, rqstStateCd)

        else:
            #stateHash = getStates(cxn)
            for state in statesByPostal.values():
                stateCd = state.stateCd
                importStateSites(cxn, stateCd)

    elif verifyGauges:
        if rqstStatePostal:
            state = statesByPostal[rqstStatePostal]
            rqstStateCd = state.stateCd
            verifyState(cxn, rqstStateCd)

        else:
            #stateHash = getStates(cxn)
            for state in statesByPostal.values():
                stateCd = state.stateCd
                verifyState(cxn, stateCd)

    else:
        doSome = True

    print("the end")

    sys.exit(0)
