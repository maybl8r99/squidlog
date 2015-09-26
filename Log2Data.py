from mongoengine import *
from Schema import *
import hashlib, time
from datetime import datetime

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
        try:
            if self.testmode==False:
                #log.save()
                pass
        except:
            print('Failed to write ' + _md5)

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
        field='domain'
        breakdown = Squidlog._get_collection().aggregate(
            [
                {
                    '$group':{
                        '_id':{
                            field:'$'+field,
                        },
                        'count':{'$sum':1},
                        'mindate':{'$min':'$datetime'},
                        'maxdate':{'$max':'$datetime'}
                    }
                }
            ]
        )
        for r in breakdown:
            #domain = r['domain']
            _date_from = r['mindate']
            _date_to = r['maxdate']
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
            try:
                if not self.testmode:
                    pass
                    #dbd.save()
            except:
                print('Failed to write ' + _md5)
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
