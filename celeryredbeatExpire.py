from celery import Celery,schedules
from redbeat import RedBeatSchedulerEntry
from threading import Timer
import pymongo
from bson.json_util import dumps
import genworker
#from celery.schedules import crontab

celery = Celery('celeryredbeat', 
                    broker='redis://localhost:6379/0',
                    redbeat_redis_url = "redis://localhost:6379/1", #URL to redis server used to store the schedule
                    beat_scheduler = 'redbeat.RedBeatScheduler',
                    beat_max_loop_interval = 5,
                    redbeat_lock_key = None)

myclient= pymongo.MongoClient("mongodb://DevApp:DevWisdomCircle2019@172.31.18.193:27018/ReactDatabase")
mongo = myclient["ReactDatabase"]

@celery.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    scheduleFlows()
    checkNewEntry()   
    cleanup_entry = RedBeatSchedulerEntry('cleanup', 'genworker.flowList', schedules.crontab(minute="*"), kwargs=({'name':'cleanup'}),app=celery)
    cleanup_entry.save()

def scheduleFlows():
    scheduledFlows = mongo.SchedulerCollection.find({'active':True}) 
    for list in scheduledFlows:
        scheduleTask(list)

def scheduleTask(list):
    interval = 0 
    cronVal = "*"
    if (list['option'] == 'sec'):
        interval = schedules.schedule(run_every=list['repeat'])
        print (interval)
    elif (list['option'] == 'min'):
        cronVal = "*/"+ str(list['repeat'])
        interval = schedules.crontab(minute=cronVal)
    elif (list['option'] == 'hour'):
        cronVal = "*/"+ str(list['repeat'])
        interval = schedules.crontab(hour=cronVal,minute=list['min'])
    elif (list['option'] == 'day'):
        if (list['repeat'] != 0):
            cronVal = "*/"+ str(list['repeat'])
        interval = schedules.crontab(day_of_month = cronVal,hour=list['hr'],minute=list['min'])
    elif (list['option'] == 'week'):
        weekArray = ['weekday-sun','weekday-mon','weekday-tue','weekday-wed','weekday-thurs','weekday-fri','weekday-sat']
        repeat_index = []
        for day in list['repeat_days']:
            day_index = weekArray.index(day)
            repeat_index.append(day_index)
            cronVal = ','.join(map(str, repeat_index)) # comma seperated string from python. map used to convert int values in array to string
            interval = schedules.crontab(day_of_week = cronVal,hour=list['hr'],minute=list['min'])
    elif (list['option'] == 'month'):
        cronVal = "*/"+ str(list['repeat'])
        interval = schedules.crontab(month_of_year=cronVal,day_of_month = list['monthly_on'],hour=list['hr'],minute=list['min'])
    elif (list['option'] == 'year'):
        interval = schedules.crontab(month_of_year=list['month_of_year'], day_of_month=list['day_of_month'], hour=list['hr'], minute=list['min'])
    
    del list['_id']
    print (list)
    entry = RedBeatSchedulerEntry(list['name'], 'genworker.flowList', interval, kwargs=(list),app=celery)
    entry.save()
    key = entry.key
    print (entry.key)
    mongo.SchedulerCollection.update_one({'name':list['name']},{'$set':{'inqueue':True,'key':key}})
    
    # if ('ends' in list and list['ends']['option'] == 'On'):
    #     print (list['ends']['option'])
    #     task_name = list['name']+'cleanup'
    #     print (task_name)
    #     dateVal = list['ends']['value']

    #     ent = RedBeatSchedulerEntry(task_name, 'cleanup_task.cleanUpTask', schedules.crontab(day_of_month = "*",hour=5,minute=52),kwargs=({'key':key,'date':dateVal,'name':task_name}),app=celery)
    #     ent.save()

def checkNewEntry():
    print ("checking")
    newSchedulers = mongo.SchedulerCollection.find({'inqueue':{'$ne':True},'active':True})
    for list in newSchedulers:
        scheduleTask(list)
    disabledSchedulers = mongo.SchedulerCollection.find({'active':False,'key':{'$exists':True}})
    for i in disabledSchedulers:
        print (i['name'])
        e = RedBeatSchedulerEntry.from_key(i['key'],app=celery)
        e.delete()
        mongo.SchedulerCollection.update_one({'name':i['name']},{'$unset':{'key':""},'$set':{'inqueue':False}})
    t = Timer(5.0,checkNewEntry)
    t.start()


