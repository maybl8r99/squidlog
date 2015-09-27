from mongoengine import *
from Schema import *
import hashlib, time
from datetime import datetime, timedelta

class Logger:
    '''
    Current velocity @ 139k/m (lines of logs processed)
    '''

    dbname = 'squidlog'
    encoding = 'latin-1'
    log_param_count = 11 # after split how many columns are expected
    m = hashlib.md5()
    conn = None
    testmode = False
    docCache = []
    docCacheLimit = 100000
    breakdownCache = []
    breakdownCacheLimit = 100000

    velocity = 139000 #current efficiency records/minute

    def __init__(self):
        self.conn = connect(self.dbname)

    def addSquidLog(self,line):
        values = line.split()
        if (len(values) != self.log_param_count):
            return False
        self.m.update(line.encode('utf-8'))
        #print(str(values[0]))
        _dt = datetime.fromtimestamp(time.mktime(time.strptime(str(values[0]),'%d/%b/%Y:%H:%M:%S')))
        _md5 = self.m.hexdigest()
        _remote_host = values[3]
        _status = values[4]
        _size_bytes = values[5]
        _method = values[6]
        _url = values[7]
        _domain = self.getDomain(_url)
        _rfc931 = values[8]
        _peer_status = values[9]
        _doc_type = values[10]
        _raw_log = line
        log = Squidlog(
            md5 = _md5,
            datetime = _dt,
            remote_host = _remote_host,
            status = _status,
            size_bytes = _size_bytes,
            method = _method,
            url = _url,
            domain = _domain,
            rfc931 = _rfc931,
            peer_status = _peer_status,
            doc_type = _doc_type,
            #raw_log = _raw_log
        )
        self.docCache.append(log)

    def getDomain(self,url):
        _url = url.replace('http://','').replace('https://','')
        values = _url.split('/')
        if '%' in values[0]:
            return ''
        return values[0]

    def defineWork(self):
        '''
        Checks for earliest datetime of log
        Checks for current datetime
        Difference between earliest and current, breakdown to days,
        Each day break down to hours (start and end)
        '''
        #earliestDate = Squidlog._get_collection().aggregate

    def getBreakdown(self,field='domain'):
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
            hour_start = current_start_date
            end_segment = current_start_date
            for i in range(24):
                end_segment = hour_start+ timedelta(hours=1)
                # load data from log where date is greater or equal to hour_start
                # and less than end_segment. Grouped by domain, count
                breakdown = Squidlog._get_collection().aggregate(
                    [
                        {
                            '$match':{'$and': [{'datetime':{'$gte':hour_start}},{'datetime':{'$lt':end_segment}}]}
                        },
                        {
                            '$group':{'_id':{'domain':'$domain',},'count':{'$sum':1},}
                        }
                    ]
                )
                # clear target
                targetClear = DomainBreakdown._get_collection(
                    [
                        {'$match':{'$and':[{'datetimefrom':{'$eq':hour_start}},{'datetimeto':{'$eq':end_segment}}],},},
                    ]
                ).delete()
                print('>>'+str(hour_start)+' -- '+str(end_segment))
                for r in breakdown:
                    #domain = r['domain']
                    _date_from = hour_start
                    _date_to = end_segment
                    _domain = r['_id']['domain']
                    _visits = r['count']
                    unique = _domain + str(_date_from) + str(_date_to)
                    self.m.update(unique.encode('utf-8'))
                    _md5 = self.m.hexdigest()
                    dbd = DomainBreakdown(
                        md5 = _md5,
                        datetimefrom = _date_from,
                        datetimeto = _date_to,
                        domain = _domain,
                        visits = _visits
                    )
                    self.breakdownCache.append(dbd)
                #end loop setting
                hour_start = end_segment
                num_records += 1
                ###
        if (len(self.breakdownCache)>0):
            print('saving')
            DomainBreakdown.objects.insert(self.breakdownCache)
            self.breakdownCache = []
        return breakdown

    def delete(self):
        if not self.testmode:
            Squidlog.drop_collection()
            DomainBreakdown.drop_collection()

    def parseFile(self,filename,logType='squid'):
        if logType == 'squid':
            with open(filename, 'r', encoding=self.encoding) as infile:
                for l in infile:
                    self.addSquidLog(l.replace('\n',''))
                    if len(self.docCache) >= self.docCacheLimit:
                        print('saving data')
                        Squidlog.objects.insert(self.docCache)
                        self.docCache = []
                # check for leftovers in cache
                if len(self.docCache) > 0:
                    Squidlog.objects.insert(self.docCache)
                    self.docCache = []
            self.getBreakdown()
            #   #   #
        else:
            raise('No log file type provided')
