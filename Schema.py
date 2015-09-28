from mongoengine import *

class Squidlog(Document):
    md5 = StringField(required=True, max_length=50, unique=True)
    datetime = DateTimeField(required=True)
    remote_host = StringField(max_length=100)
    status = StringField(max_length=50)
    size_bytes = IntField()
    method = StringField(max_length=20)
    url = StringField(max_length=100)
    domain = StringField(max_length=100)
    rfc931 = StringField(max_length=50)
    peer_status = StringField(max_length=50)
    doc_type = StringField(max_length=20)
    #raw_log = StringField()

    meta = {
        'indexes':[
            'datetime',
        ]
    }
    #meta = {
    #    'indexes' : [
    #        'md5',
    #        'datetime',
    #        'method',
    #        'domain',
    #    ]
    #}

    def count(condition):
        rec = self.objects(condition)
        return len(rec)

class DomainBreakdown(Document):
    md5 = StringField(max_length=50,unique=True)
    datetimefrom = DateTimeField()
    datetimeto = DateTimeField()
    domain = StringField(max_length=100)
    visits = IntField()

    meta = {
        'indexes':[
            'md5',
            'datetimefrom',
            'datetimeto',
            'domain',
        ]
    }
