import calendar
import time


def time_stamp():
    current_GMT = time.gmtime()
    return calendar.timegm(current_GMT)
