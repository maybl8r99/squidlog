from mongoengine import *
from Schema import *
import time
from datetime import datetime,timedelta

connect('squidlog')
total_seconds_in_a_day = 60 * 60 * 24
seconds_in_a_segment = total_seconds_in_a_day / 24
processed = 0
# start_date = earliest date in the log.
# end_date = last date in the log
start_date =  datetime.fromtimestamp(time.mktime(time.strptime('10/Sep/2015:00:00:00','%d/%b/%Y:%H:%M:%S')))
end_date = datetime.now()
#print('>>>>>>>>{}', end_date - start_date)
num_days = (end_date-start_date).days
num_records = 0
for day in range(num_days):
    current_start_date = start_date + timedelta(days=day)
    #print(current_start_date)
    #current_day = start_date + datetime.timedelta(days=1)
    #print(current_day)
    hour_start = current_start_date
    end_segment = current_start_date
    for i in range(24):
        end_segment = hour_start+ timedelta(hours=1)
        #print('Date >= '+str(hour_start)+' < '+ str(end_segment))
        # load data from log where date is greater or equal to hour_start
        # and less than end_segment. Grouped by domain, count
        breakdown = Squidlog._get_collection().aggregate(
            [
                {
                    '$match':{
                        '$and': [
                            {'datetime':{'$gte':hour_start}},
                            {'datetime':{'$lt':end_segment}}
                        ]
                    }
                },
                {
                    '$group':{
                        '_id':{
                            'domain':'$domain',
                        },
                        'count':{'$sum':1},
                    }
                }
            ]
        )
        print('>>'+str(hour_start)+' -- '+str(end_segment))
        for r in breakdown:
            if r['count'] >0:
                print ('..>'+r['_id']['domain'] +':'+str(r['count']))
        #end loop setting
        hour_start = end_segment
        num_records += 1

#delta = datetime.timedelta(end_date,start_date)
print(num_records)
