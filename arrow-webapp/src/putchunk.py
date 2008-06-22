#!/usr/bin/env python

import sys
import md5
import httplib
import base64

for f in sys.argv[1:]:
    infile = file(f)
    data = infile.read()
    m = md5.new()
    m.update(data)
    dgst = m.digest()
    path = "http://localhost:9999/store/%s" % base64.b16encode(dgst).lower()
    print "put %s..." % path
    try:
        conn = httplib.HTTPConnection('localhost', 9999)
        conn.request("PUT", path, data)
        resp = conn.getresponse()
        if resp.status is not 200:
            print "failed to put %s, code %d" % (path, resp.status)
    except:
        print "failed to put %s" % path
        pass
