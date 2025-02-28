#!/usr/bin/python3

import PostgresUtil
from DateTimeUtil import TimeInterval


################################################
# class State
################################################

class State(object):

    def __init__(self, argStateCd, argStatePostal, argStateName):

        self.stateCd = argStateCd
        self.statePostal = argStatePostal
        self.stateName = argStateName

################################################
# class County
################################################

class County(object):

    def __init__(self, argStateFips, argCountyFips, argCountyName):

        self.stateFips = argStateFips
        self.countyFips = argCountyFips
        self.countyName = argCountyName

################################################
# class Site
################################################

class Site(object):

    def __init__(self, argSiteId, argSiteName, argStateCd, argCoordLat, argCoordLong, argHydroUnitCd, \
                 argCountyCd=None, argActiveStatus=None):

        self.siteId = argSiteId
        self.siteName = argSiteName
        self.stateCd = argStateCd
        self.countyCd = argCountyCd
        self.coordLat = argCoordLat
        self.coordLong = argCoordLong
        self.hydroUnitCd = argHydroUnitCd

        self.activeStatus = argActiveStatus
        #self.lastUsgsTS = argLastUsgsTS


    ################################################
    # Site.persist
    ################################################

    def persist(self, argDbCxn):

        sql = "INSERT INTO td_site (site_id, site_name, state_cd, county_cd, coord_lat, coord_long, hydro_unit_cd) " \
              "VALUES (%(siteId)s, %(siteName)s, %(stateCd)s, %(countyCd)s, %(coordLat)s, %(coordLong)s, %(hydroUnitCd)s);"

        vals = {'siteId': self.siteId, 'siteName': self.siteName, 'stateCd': self.stateCd, 'countyCd': self.countyCd, \
                'coordLat': self.coordLat, 'coordLong': self.coordLong, 'hydroUnitCd': self.hydroUnitCd}

        cmdResult = PostgresUtil.executeStatement(argDbCxn, sql, vals)

        if cmdResult.success:
            return 0
        else:
            return 6

    ################################################
    # Site.update
    ################################################

    def update(self, argDbCxn):

        sql = "UPDATE td_site SET active_status = %(activeStatus)s, " \
                    "county_cd = %(countyCd)s, update_ts = CURRENT_TIMESTAMP WHERE site_id = %(siteId)s;"

        vals = { 'activeStatus': self.activeStatus,  'countyCd': self.countyCd, 'siteId': self.siteId}

        cmdResult = PostgresUtil.executeStatement(argDbCxn, sql, vals)

        result = cmdResult.success

        return result

    ################################################
    # Site.find
    ################################################

    def find(argDbCxn, siteNo):

        sql = "SELECT site_id, site_name, state_cd, county_cd, coord_lat, \
            coord_long, hydro_unit_cd, active_status, create_ts, update_ts \
            from td_site where site_id = '{}'".format(siteNo)

        try:
            cur = argDbCxn.cursor()

            cur.execute(sql)
            row = cur.fetchone()

            siteId = row[0]
            siteName = row[1]
            stateCd = row[2]
            countyCd = row[3]
            coordLat = row[4]
            coordLong = row[5]
            hydroUnitCd = row[6]
            activeStatus = row[7]

            result = Site(siteId, siteName, stateCd, coordLat, coordLong, hydroUnitCd, countyCd, activeStatus)

        finally:
            cur.close()

        return result

################################################
# class Gauge
################################################

class Gauge(object):

    def __init__(self, argGaugeId, argSiteId, argGaugeType, argQualifs, argActiveStatus=None, argLastVal=None, argLastUsgsTS=None):

        self.gaugeId = argGaugeId
        self.siteId = argSiteId
        self.gaugeType = argGaugeType
        self.qualifs = argQualifs
        self.activeStatus = argActiveStatus
        self.lastVal = argLastVal
        self.lastUsgsTS = argLastUsgsTS

    ################################################
    # Gauge.persist
    ################################################

    def persist(self, argDbCxn):

        sql = "INSERT INTO td_gauge (gauge_id, site_id, gauge_type, active_status, qualifs, last_val, last_usgs_ts) " \
              "VALUES (%(gaugeId)s, %(siteId)s, %(gaugeType)s, %(activeStatus)s, %(qualifs)s, %(lastVal)s, %(lastUsgsTS)s);"

        vals = {'gaugeId': self.gaugeId, 'siteId': self.siteId, 'gaugeType': self.gaugeType, \
                'activeStatus': self.activeStatus, 'lastVal': self.lastVal, \
                'lastUsgsTS': self.lastUsgsTS, 'qualifs' : self.qualifs}

        cmdResult = PostgresUtil.executeStatement(argDbCxn, sql, vals)

        if cmdResult.success:
            return 0
        else:
            return 6

    ################################################
    # Gauge.update
    ################################################

    def update(self, argDbCxn):

        sql = "UPDATE td_gauge SET active_status = %(activeStatus)s, " \
            "last_val = %(lastVal)s, last_usgs_ts = %(lastUsgsTS)s, qualifs = %(qualifs)s, " \
            "update_ts = CURRENT_TIMESTAMP WHERE gauge_id = %(gaugeId)s;"

        vals = { 'activeStatus': self.activeStatus, 'lastUsgsTS': self.lastUsgsTS, \
                 'lastVal': self.lastVal, 'qualifs': self.qualifs, 'gaugeId': self.gaugeId }

        cmdResult = PostgresUtil.executeStatement(argDbCxn, sql, vals)

        result = cmdResult.success

        return result

################################################
# class GaugeCheckin
################################################

class GaugeCheckin(object):

    def __init__(self, argGaugeId, argUsgsTS, argCheckinVal, argQualifs):

        self.gaugeId = argGaugeId
        self.usgsTS = argUsgsTS
        self.checkinVal = argCheckinVal
        self.qualifs = argQualifs
        self.stage = None

    ################################################
    # GaugeCheckin.persist
    ################################################

    def persist(self, argDbCxn):

        sql = "INSERT INTO td_gauge_checkin (gauge_id, usgs_ts, checkin_val, qualifs) " \
              "VALUES (%(gaugeId)s, %(usgsTS)s, %(checkinVal)s, %(qualifs)s);"

        vals = {'gaugeId': self.gaugeId, 'usgsTS': self.usgsTS, \
                'checkinVal': self.checkinVal, 'qualifs': self.qualifs }

        cmdResult = PostgresUtil.executeStatement(argDbCxn, sql, vals)



        if cmdResult.success:
            return 0
        else:
            return 6

################################################
# class LevelStage
################################################

class LevelStage(object):

    def __init__(self, argOrdinal, argLabel, argLevelMin, argRunnable):

        self.ordinal = argOrdinal
        self.label = argLabel
        self.levelMin = argLevelMin
        self.isRunnable = argRunnable


################################################
# class DailyStat
################################################

class DailyStat(object):

    def __init__(self, argDate, argAstronomyData=None):

        self.date = argDate
        self.astronomyData = argAstronomyData
        self.events = []

    def hasEvents(self):
        result = len(self.events) > 0
        return result

    def addEvent(self, argEvent):
        self.events.append(argEvent)

    def getRunnableRatio(self):

        daylightMins = self.astronomyData.getSpanInMins()

        totalRunnableMins = 0

        for event in self.events:
            eventSpanMins = event.interval.getSpanInMins()
            totalRunnableMins += eventSpanMins

        prelimin = (totalRunnableMins / daylightMins) * 100
        result = min(prelimin, 100)

        return result

################################################
# class LevelEvent
################################################

class LevelEvent(object):

    def __init__(self):

        #self.startTime = argStartTime
        #self.endTime = None

        self.interval = None

        #self.stageLabel = argStageLabel
        self.checkins = []
        self.minCheckin = None
        self.maxCheckin = None
        self.valSum = 0
        self.avgCheckinVal = None
        self.stageList = []
        self.stageDisplay = ""



    def closeEvent(self, argEndTime):
        #self.endTime = argEndTime
        self.interval.end = argEndTime

        self.avgCheckinVal = float(self.valSum) / float(len(self.checkins))

    def addStageX(self, argStage):

        if len(self.stageList) > 0:
            self.stageDisplay += "|"

        self.stageDisplay += argStage.label

        self.stageList.append(argStage)


    def addCheckin(self, argCheckin):

        latestStage = None

        if len(self.stageList) > 0:
            latestStage = self.stageList[-1]

        if argCheckin.stage and (not latestStage or argCheckin.stage.ordinal != latestStage.ordinal):

            if len(self.stageList) > 0:
                self.stageDisplay += "|"

            self.stageList.append(argCheckin.stage)


            self.stageDisplay += argCheckin.stage.label


        if len(self.checkins) == 0:
            self.interval = TimeInterval(argCheckin.usgsTS)

        self.checkins.append(argCheckin)

        checkinVal = argCheckin.checkinVal

        self.valSum += checkinVal

        if self.minCheckin:
            minCheckinVal = self.minCheckin.checkinVal

            if checkinVal < minCheckinVal:
                self.minCheckin = argCheckin
        else:
            self.minCheckin = argCheckin

        if self.maxCheckin:
            maxCheckinVal = self.maxCheckin.checkinVal

            if checkinVal > maxCheckinVal:
                self.maxCheckin = argCheckin
        else:
            self.maxCheckin = argCheckin

    def __repr__(self):

        startTimeStr = self.interval.start.strftime("%Y-%m-%dT%H:%M:%S")
        endTimeStr = self.interval.end.strftime("%Y-%m-%dT%H:%M:%S")
        spanMins = self.interval.getSpanInMins()
        dayOfWeek = self.interval.end.strftime("%A")

        result = "-" * 81 + "\nLEVEL EVENT (" + self.stageDisplay + ")" \
            "\nSTART " + startTimeStr + \
            "\nEND " + endTimeStr + \
            "\nWEEKDAY " + dayOfWeek + \
            "\nDURATION (mins) " + str(spanMins) + \
            "\nMIN " + str(self.minCheckin.checkinVal) + \
            "\nMAX " + str(self.maxCheckin.checkinVal) + \
            "\nAVG " + str(self.avgCheckinVal) #+ \
            #"\nCHECKINS " + str(len(self.checkins))



        return result

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
