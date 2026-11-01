"""Bounded UTC partition windows shared by backfill validation and SQL templates."""
from datetime import date,datetime,timedelta,timezone
import re


def partition_window(value,granularity='day'):
    try:
        if granularity=='day':
            if not re.fullmatch(r'\d{4}-\d{2}-\d{2}',value):raise ValueError('Daily partitions require YYYY-MM-DD dates.')
            start=datetime.combine(date.fromisoformat(value),datetime.min.time(),timezone.utc)
            delta=timedelta(days=1);key=start.date().isoformat()
        elif granularity=='hour':
            start=datetime.fromisoformat(value.replace('Z','+00:00'))
            if start.tzinfo is None or start.utcoffset() is None:raise ValueError('Hourly partitions require a timestamp with Z or an explicit UTC offset.')
            start=start.astimezone(timezone.utc)
            if start.minute or start.second or start.microsecond:raise ValueError('Hourly partitions must align to a whole UTC hour.')
            delta=timedelta(hours=1);key=start.isoformat()
        else:raise ValueError('Choose daily or hourly partitions.')
        end=start+delta
    except OverflowError as exc:raise ValueError('Partition window exceeds the supported date range.') from exc
    return {'granularity':granularity,'key':key,'date':start.date().isoformat(),'start':start.isoformat(),'end':end.isoformat()}


def backfill_windows(start,end,granularity):
    first,last=partition_window(start,granularity),partition_window(end,granularity)
    begin,finish=datetime.fromisoformat(first['start']),datetime.fromisoformat(last['start'])
    delta=timedelta(days=1) if granularity=='day' else timedelta(hours=1)
    count=(finish-begin)//delta+1
    maximum=31 if granularity=='day' else 48
    if not 1<=count<=maximum:raise ValueError(f'Backfills require 1–{maximum} inclusive {"daily" if granularity=="day" else "hourly"} partitions.')
    return [partition_window((begin+delta*i).date().isoformat() if granularity=='day' else (begin+delta*i).isoformat(),granularity) for i in range(count)]
