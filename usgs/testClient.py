#!/usr/bin/python3

import ServiceUtil
import DateTimeUtil
import time
from datetime import datetime

################################################
# main
################################################

if __name__ == '__main__':

    print("in testClient")

    rqstDate = datetime.strptime("2024-9-10", "%Y-%m-%d")

    #ServiceUtil.fetchAstronomyData(39.5258056, -79.4107500, rqstDate)