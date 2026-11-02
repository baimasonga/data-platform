"""Bounded UTC partition windows shared by backfills, catch-up and SQL templates."""
from datetime import date,datetime,timedelta,timezone
import re

LIMITS={'day':31,'hour':48,'week':26,'month':24}
LABELS={'day':'daily','hour':'hourly','week':'weekly','month':'monthly'}
DELTAS={'hour':timedelta(hours=1),'day':timedelta(days=1),'week':timedelta(days=7)}


def advance(start,granularity,count):
    try:
        if granularity=='month':
            index=start.year*12+start.month-1+count
            return start.replace(year=index//12,month=index%12+1)
        return start+DELTAS[granularity]*count
    except (OverflowError,ValueError) as exc:raise ValueError('Partition window exceeds the supported date range.') from exc


def partition_window(value,granularity='day'):
    if granularity in ('day','week','month'):
        if not re.fullmatch(r'\d{4}-\d{2}-\d{2}',value):raise ValueError('Calendar partitions require YYYY-MM-DD start dates.')
        start=datetime.combine(date.fromisoformat(value),datetime.min.time(),timezone.utc)
        if granularity=='week' and start.weekday()!=0:raise ValueError('Weekly partitions must start on a Monday in UTC.')
        if granularity=='month' and start.day!=1:raise ValueError('Monthly partitions must start on the first day of a calendar month in UTC.')
        key=start.date().isoformat()
    elif granularity=='hour':
        try:
            start=datetime.fromisoformat(value.replace('Z','+00:00'))
            if start.tzinfo is None or start.utcoffset() is None:raise ValueError('Hourly partitions require a timestamp with Z or an explicit UTC offset.')
            start=start.astimezone(timezone.utc)
        except OverflowError as exc:raise ValueError('Partition window exceeds the supported date range.') from exc
        if start.minute or start.second or start.microsecond:raise ValueError('Hourly partitions must align to a whole UTC hour.')
        key=start.isoformat()
    else:raise ValueError('Choose daily, hourly, weekly or monthly partitions.')
    end=advance(start,granularity,1)
    return {'granularity':granularity,'key':key,'date':start.date().isoformat(),'start':start.isoformat(),'end':end.isoformat()}


def distance(begin,end,granularity):
    if granularity=='month':return (end.year-begin.year)*12+end.month-begin.month
    return (end-begin)//DELTAS[granularity]


def windows_from(begin,granularity,count):
    starts=[advance(begin,granularity,index) for index in range(count)]
    return [partition_window(start.isoformat() if granularity=='hour' else start.date().isoformat(),granularity) for start in starts]


def backfill_windows(start,end,granularity):
    first,last=partition_window(start,granularity),partition_window(end,granularity)
    begin,finish=datetime.fromisoformat(first['start']),datetime.fromisoformat(last['start'])
    count=distance(begin,finish,granularity)+1
    maximum=LIMITS[granularity]
    if not 1<=count<=maximum:raise ValueError(f'Backfills require 1–{maximum} inclusive {LABELS[granularity]} partitions.')
    return windows_from(begin,granularity,count)


def catchup_windows(start,granularity,current):
    """Plan only completed UTC windows without allocating the entire backlog."""
    first=partition_window(start,granularity)
    begin=datetime.fromisoformat(first['start'])
    due=max(0,distance(begin,current.astimezone(timezone.utc),granularity))
    return windows_from(begin,granularity,min(due,LIMITS[granularity])),due
