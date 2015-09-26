
from mongoengine import *
from Schema import *
import hashlib, time, sys, getopt
from datetime import datetime
from dateutil import parser
import Log2Data

logfile = 'access.log-20150920'
filelength = 0

def stat():
    all_get = Squidlog.objects(method='GET').count()
    all_post = Squidlog.objects(method='POST').count()
    total = Squidlog.objects().count()
    print('ALL:'+str(total))
    print('GET:' + str(all_get))
    print('POST:' + str(all_post))
    print('REMAINDER:'+str(total-(all_get+all_post)))

def breakdownBy(field):
    breakdown = SL.getBreakdown(field)
    #print('Size:'+len(breakdown))
    for i in breakdown:
        print(i)

def usage():
    print('''
        USAGE:
            -i      Init database
            -r      Refresh database
            -s      Database statistics
            -d      Domain breakdown
            -t      Test mode - no writes
        ''')

def file_len(fname):
    with open(fname, 'r',encoding='utf-8', errors='ignore') as f:
        for i, l in enumerate(f):
            pass
    return i + 1


if __name__ == '__main__':
    opts = {}
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'thisrd',['init','status'])
    except getopt.GetoptError as err:
        print(str(err))
        usage()
    start_time = datetime.now()

    SL = Log2Data.Logger()
    #SL.parseFile('access.log-20150916')
    #connect('squidlog_test')
    if '-t' in opts:
        SL.testmode = True
        print ('Test only')
    for o, a in opts:
        if o == '-h':
            usage()
        else:
            filelength = file_len(logfile)
            print('Total lines to process: '+ str(filelength) + '\nEstimated time: ' + str(filelength / 139000) + ' minutes')
            if o == '-t':
                SL.testmode = True
                print('Test only')
            if o == '-i':
                SL.delete()
                SL.parseFile(logfile)
            if o == '-r':
                SL.parseFile(logfile)
            if o == '-s':
                stat()
            if o == '-d':
                breakdownBy('domain')

    finish_time = datetime.now()
    elapsed_time = finish_time - start_time
    print(str(elapsed_time) + ' seconds')
