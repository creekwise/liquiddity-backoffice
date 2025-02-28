#!/usr/bin/python3

################################################
# class TimeInterval
################################################

class TimeInterval(object):

    def __init__(self, argStart, argEnd=None):

        if argEnd and not argEnd > argStart:
            raise Exception("start date must be before end date")

        self.start = argStart
        self.end = argEnd

    def getSpanInMins(self):
        span = self.end - self.start
        spanSecs = span.total_seconds()

        result = spanSecs/60
        return result

################################################
# getIntervalIntersection
################################################

def getIntervalIntersection(argIntervalA, argIntervalB):

    result = None
    sectStart = None

    if argIntervalA.start < argIntervalB.start:
        sectStart = argIntervalB.start
    else:
        sectStart = argIntervalA.start

    sectEnd = None

    if argIntervalA.end < argIntervalB.end:
        sectEnd = argIntervalA.end
    else:
        sectEnd = argIntervalA.start

    if sectEnd > sectStart:
        result = TimeInterval(sectStart, sectEnd)

    return result