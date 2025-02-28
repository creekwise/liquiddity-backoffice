#!/usr/bin/python3

import requests
import json
import time
from datetime import datetime
import pytz

from DateTimeUtil import TimeInterval


################################################
# fetchAstronomyData
################################################

def fetchAstronomyData(argLat, argLong, argDate):
    siteUrlTempl = "https://api.sunrisesunset.io/json?lat={}&lng={}&date={}"

    dateTZ = argDate.tzinfo

    dateStr = argDate.strftime("%Y-%m-%d")

    siteUrl = siteUrlTempl.format(argLat, argLong, dateStr)


    response = requests.get(siteUrl)
    respText = response.content.decode("utf-8");

    topJsonStruct = json.loads(respText)

    resultStruct = topJsonStruct["results"]
    sunriseTimeStr = resultStruct["sunrise"]
    sunsetTimeStr = resultStruct["sunset"]
    timezoneStr = resultStruct["timezone"]
    timezone = pytz.timezone(timezoneStr)

    serviceDateTimeFormat = "%Y-%m-%dT%I:%M:%S %p"

    sunriseStr = "{}T{}".format(dateStr, sunriseTimeStr)
    sunriseNaive = datetime.strptime(sunriseStr, serviceDateTimeFormat)

    sunriseAware = timezone.localize(sunriseNaive)

    sunriseTZ = sunriseAware.tzinfo

    sunsetStr = "{}T{}".format(dateStr, sunsetTimeStr)
    sunsetNaive = datetime.strptime(sunsetStr, serviceDateTimeFormat)

    sunsetAware = timezone.localize(sunsetNaive)


    #print("-" * 81)
    #print("sunrise " + sunriseStr)
    #print("sunset " + sunsetStr)

    result = TimeInterval(sunriseAware, sunsetAware)

    spanMins = result.getSpanInMins()
    daylightHrs = spanMins/60

    #print("daylightHrs " + str(daylightHrs))

    return result