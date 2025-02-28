#!/bin/python3

###################################################################################################
# Description goes here
###################################################################################################

import sys
import configparser
import argparse
from datetime import datetime
from os import MFD_ALLOW_SEALING

from UbuntuDrivers.detect import lookup_cache
from cupshelpers.debug import nonfatalException
from pyatspi import findAllDescendants

# import requests
# import xml.etree.ElementTree as ET

import ConfigUtil
import CommonUtil
import PostgresUtil
import JobUtil
import LiquiddityModel
from LiquiddityModel import State, Site, Gauge, GaugeCheckin, LevelStage, LevelEvent, DailyStat
import ServiceUtil

import statistics
import time
import datetime

################################################
# createJobArgParser
################################################

def createJobArgParser():
    result = argparse.ArgumentParser()

    result.add_argument('--props', help='Properties file path', required=True)
    result.add_argument('-l', '--logDir', help='Log file', required=True)
    #result.add_argument('-g', '--gaugeId', help='Gauge ID', required=True)
    result.add_argument('-s', '--startDate', help='Start date', required=True)
    result.add_argument('-e', '--endDate', help='End date', required=False)

    return result

################################################
# getCheckins
################################################

def getCheckins(argConn, argGaugeId, argStartDate, argEndDate):
    result = {}

    # stateClause = " AND s.state_cd = '{}'".format(argStatePostal) if argStatePostal else ""


    startDateSql = time.strftime("%Y-%m-%dT%H:%M:%S", argStartDate)

    if argEndDate:
        endDateSql = time.strftime("%Y-%m-%dT%H:%M:%S", argEndDate)
        endDateClause = " AND usgs_ts < '{}'".format(endDateSql)
    else:
        endDateClause = ""

    try:
        cur = argConn.cursor()

        sql = ("SELECT usgs_ts, checkin_val, qualifs \
                    FROM td_gauge_checkin WHERE gauge_id = %(gaugeId)s AND usgs_ts >= %(startDate)s {} \
                    ORDER BY usgs_ts;".format(endDateClause))

        vals = {'gaugeId': argGaugeId, 'startDate': startDateSql}

        cur.execute(sql, vals)
        rows = cur.fetchall()

        for row in rows:
            usgsTs = row[0]
            checkinVal = row[1]
            qualifs = row[2]

            #print("usgsTs as read from DB = " + str(usgsTs))

            checkin = GaugeCheckin(argGaugeId, usgsTs, checkinVal, qualifs)

            result[usgsTs] = checkin

    finally:
        cur.close()

    return result

################################################
# detectEvents
################################################
def detectEvents(argSite, argCheckins, argStages):

    checkinTimeKeys = list(argCheckins.keys())

    checkinTimeKeys.sort()

    siteLat = argSite.coordLat
    siteLong = argSite.coordLong

    dailyStatData = {}

    event = None
    #closeEvent = False

    for checkinTimeKey in checkinTimeKeys:
        checkinDate = checkinTimeKey.date()

        dailyStat = dailyStatData.get(checkinDate)

        if dailyStat:
            daylightInterval = dailyStat.astronomyData
        else:
            print("Processing events for {}".format(str(checkinDate)))
            daylightInterval = ServiceUtil.fetchAstronomyData(siteLat, siteLong, checkinTimeKey)
            dailyStat = DailyStat(checkinDate, daylightInterval)
            dailyStatData[checkinDate] = dailyStat

        checkin = argCheckins[checkinTimeKey]

        closeEvent = False

        if checkin.qualifs == 'P':

            stage = findLevelStage(argStages, checkin.checkinVal)

            if stage is None:
                raise Exception("Unable to find stage for level {}".format(checkin.checkinVal))

            checkin.stage = stage

            isDaylight = checkinTimeKey >= daylightInterval.start and checkinTimeKey <= daylightInterval.end

            if stage.isRunnable and isDaylight:

                if not event:
                    event = LevelEvent()

                event.addCheckin(checkin)
            else:

                if event:
                    closeEvent = True

        else:

            if event:
                closeEvent = True

        if closeEvent:
            #eventCloseTime = None

            if checkinTimeKey < daylightInterval.end:
                eventCloseTime = checkinTimeKey
            else:
                eventCloseTime = daylightInterval.end

            #event.closeEvent(checkinTimeKey)
            event.closeEvent(eventCloseTime)

            if event.interval.getSpanInMins() >= 120:
                #print(event)
                # eventList.append(event)
                dailyStat.addEvent(event)
                #dailyStatData[checkinDate] = dailyStat

            event = None

    return dailyStatData

################################################
# detectReleases
################################################

def detectReleases(argCheckins, argDischarge, argInflowMins, argOutflowMins):

    checkinTimeKeys = list(argCheckins.keys())

    checkinTimeKeys.sort()

    result = {}

    release = None
    #closeRelease = False

    for checkinTimeKey in checkinTimeKeys:
        checkinDate = checkinTimeKey.date()

        dailyStat = result.get(checkinDate)

        if not dailyStat:
            print("Processing releases for {}".format(str(checkinDate)))
            dailyStat = DailyStat(checkinDate)
            result[checkinDate] = dailyStat

        checkin = argCheckins[checkinTimeKey]

        closeRelease = False

        inflowTimeDelta = datetime.timedelta(minutes=argInflowMins)
        inflowLookupTime = checkinTimeKey - inflowTimeDelta
        inflowDeltaCheckin = argCheckins.get(inflowLookupTime)

        outflowTimeDelta = datetime.timedelta(minutes=argOutflowMins)
        outflowLookupTime = checkinTimeKey - outflowTimeDelta
        outflowDeltaCheckin = argCheckins.get(outflowLookupTime)

        #print("checkinTimeKey = {}".format(str(checkinTimeKey)))
        #print("lookupTime = {}".format(str(lookupTime)))

        acceptableQualifs = set(['P', 'A'])

        if (checkin.qualifs in acceptableQualifs and inflowDeltaCheckin and outflowDeltaCheckin
                and inflowDeltaCheckin.qualifs in acceptableQualifs):

            inflowDelta = checkin.checkinVal - inflowDeltaCheckin.checkinVal
            outflowDelta = checkin.checkinVal - outflowDeltaCheckin.checkinVal

            if inflowDelta > argDischarge:

                if not release:
                    release = LevelEvent()

                release.addCheckin(checkin)
            elif outflowDelta < (argDischarge * (-1)) and release:

                closeRelease = True

       # else:

        #    if release:
        #        closeRelease = True

        if closeRelease:
            release.closeEvent(checkinTimeKey)
            dailyStat.addEvent(release)
            release = None

    return result

################################################
# findLevelStage
################################################
def findLevelStage(argStages, argLevel):

    result = None
    stageCount = len(argStages)

    for stageOrdinal in range(0, stageCount):
        stage = argStages[stageOrdinal]
        stageMin = stage.levelMin

        if argLevel >= stageMin:
            result = stage

    return result

################################################
# getStages
################################################
def getStages(argFlowConf):

    result = {}

    flowConfKeys = argFlowConf.keys()

    for flowConfKey in flowConfKeys:
        if flowConfKey.startswith("stage."):
            stageId = flowConfKey[-(len(flowConfKey) - 6):]
            stageIdElems = stageId.split(".")
            stageOrdStr = stageIdElems[0]
            stageLabel = stageIdElems[1]
            isRunnableStr = stageIdElems[2]
            stageMinStr = argFlowConf[flowConfKey]
            stageMin = int(stageMinStr)
            isRunnable = bool(int(isRunnableStr))
            print("stageId = " + stageId)
            print("stageOrd = " + stageOrdStr)
            print("stageLabel = " + stageLabel)
            print("stageMin = " + stageMinStr)
            print("isRunnable = " + str(isRunnable))

            stageOrd = int(stageOrdStr)

            stage = LevelStage(stageOrd, stageLabel, stageMin, isRunnable)
            result[stageOrd] = stage

    zeroStage = result.get(0)

    if not zeroStage:
        zeroStage = LevelStage(0, "TOO-LOW", 0, False)
        result[0] = zeroStage

    return result



################################################
# main
################################################

if __name__ == '__main__':

    print("in processLevelEvents")

    argparser = createJobArgParser()

    args = argparser.parse_args()

    propFile = args.props
    #gaugeId = args.gaugeId
    startDateStr = args.startDate
    endDateStr = args.endDate

    #print("Gauge ID = " + gaugeId)


    startDate = time.strptime(startDateStr, "%Y-%m-%d")
    startDateDisp = CommonUtil.getTimeDisplay(startDate)
    print("startDate = " + startDateDisp)

    endDate = None

    if endDateStr:
        endDate = time.strptime(endDateStr, "%Y-%m-%d")
        endDateDisp = CommonUtil.getTimeDisplay(endDate)
        print("endDate = " + endDateDisp)

    config = configparser.ConfigParser()
    config.read(propFile)

    fileSysConfHash = {}
    fileSysConfHash['logDir'] = args.logDir

    fileSysConf = ConfigUtil.getFileSysConfig(fileSysConfHash)

    jobConfig = JobUtil.initJob("LEVEL EVENT PROCESSOR", "manual", fileSysConf)
    jobLogger = jobConfig.logger

    dbConnConf = ConfigUtil.getPostgresConfig(config['DB-CONN'])

    cxn = PostgresUtil.getConnection(dbConnConf, jobLogger)

    flowConf = config['FLOW']

    gaugeId = flowConf['gaugeId']
    releaseDischargeStr = flowConf.get('releaseDischarge')

    releaseDischarge = None
    releaseInflowMins = None
    releaseOutflowMins = None

    if releaseDischargeStr:
        releaseDischarge = int(releaseDischargeStr)
        releaseInflowMinsStr = flowConf.get('releaseInflowMins')
        releaseOutflowMinsStr = flowConf.get('releaseOutflowMins')
        releaseInflowMins = int(releaseInflowMinsStr)
        releaseOutflowMins = int(releaseOutflowMinsStr)

    stages = getStages(flowConf)

    checkins = getCheckins(cxn, gaugeId, startDate, endDate)

    if releaseDischarge:
        releases = detectReleases(checkins, releaseDischarge, releaseInflowMins, releaseOutflowMins)
    #exit(0)

    gaugeIdElems = gaugeId.split(":")
    siteId = gaugeIdElems[0]
    site = Site.find(cxn, siteId)

    dailyStats = detectEvents(site, checkins, stages)

    dailyStatKeys = list(dailyStats.keys())

    dailyStatKeys.sort()

    eventDayIndx = 0

    outHeader = "EVENT_INDEX|RELEASE_INDEX|DATE|WEEKDAY|SUNRISE|SUNSET|START|END|DAYLIGHT_PCT|DURATION_mins|MIN_FLOW|MAX_FLOW|AVG_FLOW"
    outLineTempl = "{event_index}|{release_index}|{date}|{weekday}|{sunrise}|{sunset}|{start}|{end}|{daylight_pct}|{duration}|{min_flow}|{max_flow}|{avg_flow}"

    print(outHeader)

    debug = False

    eventIndx = 0
    releaseIndx = 0

    for dailyStatDate in dailyStatKeys:
        dailyStat = dailyStats[dailyStatDate]
        dailyEventCount = len(dailyStat.events)


        if dailyEventCount > 0:
            eventDayIndx += 1
            runRatio = int(round(dailyStat.getRunnableRatio(), 0))

            if releaseDischarge:
                releaseDailyStat = releases.get(dailyStatDate)

                if releaseDailyStat:
                    if releaseDailyStat.hasEvents():
                        #mode = "REL"
                        releaseIndx += 1
                        releaseIndxDisp = str(releaseIndx)
                    else:
                        #mode = "NAT"
                        releaseIndxDisp = "N/A"
            else:
                #mode = "N/A"
                releaseIndxDisp = "N/A"

            if debug:
                print("-" * 81)
                print("event day {} = {}".format(str(eventDayIndx), str(dailyStatDate)))
                print("daily events = " + str(dailyEventCount))
                print("daily runnable ratio = " + str(runRatio))
                print(mode)
                print()

            for event in dailyStat.events:

                eventIndx += 1

                if debug:
                    print(event)

                weekday = dailyStatDate.strftime("%A")
                sunrise = dailyStat.astronomyData.start.strftime("%H:%M")
                sunset = dailyStat.astronomyData.end.strftime("%H:%M")
                start = event.interval.start.strftime("%H:%M")
                end = event.interval.end.strftime("%H:%M")
                spanMins = int(round(event.interval.getSpanInMins(), 0))
                minFlow = int(round(event.minCheckin.checkinVal, 0))
                maxFlow = int(round(event.maxCheckin.checkinVal, 0))
                avgFlow = int(round(event.avgCheckinVal, 0))

                outLine = outLineTempl.format(event_index=eventIndx, \
                                              release_index=releaseIndxDisp, \
                                              date=dailyStatDate, \
                                              weekday=weekday, \
                                              sunrise=sunrise, \
                                              sunset=sunset, \
                                              start=start, \
                                              end=end, \
                                              daylight_pct=runRatio, \
                                              duration=spanMins, \
                                              min_flow=minFlow, \
                                              max_flow=maxFlow, \
                                              avg_flow=avgFlow)

                print(outLine)

    JobUtil.endJob(jobConfig)