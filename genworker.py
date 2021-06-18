from celery import Celery
import pymongo
from redbeat import RedBeatSchedulerEntry
import celeryredbeatExpire
import datetime

app = Celery('genworker', broker='redis://localhost:6379/0', backend='redis://localhost:6379/0')

myclient= pymongo.MongoClient("mongodb://DevApp:DevWisdomCircle2019@172.31.18.193:27018/ReactDatabase")
mongo = myclient["ReactDatabase"]

@app.task(queue='workerqueue')
def flowList(**kwargs):
    name = kwargs['name']
    print (name)
    
    if (name == 'cleanup'):
        cleanSchedulers = mongo.SchedulerCollection.find({'active':True})
        for cleanSch in cleanSchedulers:
            if ('ends' in cleanSch and cleanSch['ends']['option'] == 'On'):
                today =datetime.date.today()
                onDate = datetime.datetime.strptime(cleanSch['ends']['value'],'%Y-%m-%d')
                if (today == onDate.date()):
                    ent = RedBeatSchedulerEntry.from_key(cleanSch['key'],app=celeryredbeatExpire.celery)
                    ent.delete()
                    mongo.SchedulerCollection.update_one({'name':cleanSch['name']},{'$unset':{'key':""},'$set':{'inqueue':False,'active':False}})
    else :
        task_arg = {}
        task_arg['str_reqNo']= 10
        task_arg['flowname'] = kwargs["flow"][0]
        argkey = kwargs["nodes"][0].keys()
        for arg in argkey:
            task_arg[arg] = kwargs["nodes"][0][arg]
        print (task_arg)
        r = app.send_task('sshtask.flowList', kwargs=(task_arg),queue='sshqueue')
        print (r.id)
        schDetail = mongo.SchedulerCollection.find_one({'name':name}) 
        if ('occurence' in schDetail and schDetail['occurence'] == 1):  
            e = RedBeatSchedulerEntry.from_key(schDetail['key'],app=celeryredbeatExpire.celery)
            e.delete()
            mongo.SchedulerCollection.update_one({'name':schDetail['name']},{'$unset':{'key':""},'$set':{'inqueue':False,'active':False}})
        elif ('ends' in schDetail):
            if (schDetail['ends']['option'] == 'After'):
                occurence = schDetail['ends']['value']
                counter = 1
                if ('counter' in schDetail):
                    counter = schDetail['counter'] +1

                if (counter < occurence):
                    mongo.SchedulerCollection.update_one({'name':kwargs['name']},{'$set':{'counter':counter}})
                else:
                    e = RedBeatSchedulerEntry.from_key(schDetail['key'],app=celeryredbeatExpire.celery)
                    e.delete()
                    mongo.SchedulerCollection.update_one({'name':schDetail['name']},{'$unset':{'key':"",'counter':""},'$set':{'inqueue':False,'active':False}})

           