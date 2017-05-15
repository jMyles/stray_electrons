import pytz, datetime

HOUR_OF_DAY_CHANGE = 4

UTC_OFFSET_TIMEDELTA = datetime.datetime.now().replace(minute=0, second=0,
                                       microsecond=0) - datetime.datetime.utcnow().replace(minute=0,
                                                                                           second=0,
                                                                                           microsecond=0)


def first_moment_today_utc():
    now = datetime.datetime.now()

    if now.hour < HOUR_OF_DAY_CHANGE:
        now = now - datetime.timedelta(days=1)

    first_moment_today = now.replace(hour=HOUR_OF_DAY_CHANGE, minute=0, second=0, microsecond=0)


    first_moment_today_utc = first_moment_today - UTC_OFFSET_TIMEDELTA
    return first_moment_today_utc