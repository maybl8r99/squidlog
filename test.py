import time
from datetime import datetime

total_seconds_in_a_day = 60 * 60 * 24
seconds_in_a_segment = total_seconds_in_a_day / 24
processed = 0
start_date =  datetime.fromtimestamp(time.mktime(time.strptime('1/Jan/2015:00:00:00','%d/%b/%Y:%H:%M:%S')))
end_date = datetime.now()
while True:
    print('R:'+str(processed)+' to '+str(processed+seconds_in_a_segment))
    hour_start = 0
    for i in range(24):
        end_segment = hour_start+seconds_in_a_segment
        print('>'+str(hour_start)+' -- '+ str(end_segment))
        hour_start = end_segment

    processed = processed + seconds_in_a_segment
    if (processed > total_seconds_in_a_day):
        break

#delta = datetime.timedelta(end_date,start_date)
print((end_date-start_date).days)
