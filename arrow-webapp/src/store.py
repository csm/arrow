#!/usr/bin/env python
#
# Copyright (C) 2008  Casey Marshall
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.


import wsgiref.handlers
from google.appengine.ext import webapp
from google.appengine.ext import db
from google.appengine.api import memcache

import arrow
import chunk

class StoreHandler(webapp.RequestHandler):
    def get_chunk(self, id):
        """
        Fetch a chunk from the store.
        """
        # Note that memcache seems to not work with complex objects (though,
        # a Chunk is not exactly complicated). FIXME: investigate this.
        #cachekey = "chunk/%s" % id
        #chunk = memcache.get(cachekey)
        #if chunk is not None:
        #    return chunk
        query = Chunk.gql("WHERE id = :1", id)
        chunk = query.get()
        #if chunk is not None:
        #    memcache.add(cachekey, chunk, 60)
        return chunk

    # Head tells you if this chunk exists or not.
    def head(self):
        id = getid(self.request.path)
        chunk = self.get_chunk(id)
        if chunk is not None:
            self.response.headers['X-Arrow-ContentLength'] = len(chunk.data)
            if chunk.parity is not None:
                self.response.headers['X-Arrow-Parity'] = str(chunk.parity)
            self.response.headers['X-Arrow-RefCount'] = str(chunk.refcount)
            self.response.headers['X-Arrow-Weaksum'] = str(chunk.weaksum)
        else:
            self.response.status = 404
    
    # Fetches a chunk.
    def get(self):
        id = getid(self.request.path)
        chunk = self.get_chunk(id)
        if chunk is not None:
            self.response.headers['X-Arrow-ContentLength'] = str(len(chunk.data))
            if chunk.parity is not None:
                self.response.headers['X-Arrow-Parity'] = str(chunk.parity)
            self.response.headers['X-Arrow-RefCount'] = str(chunk.refcount)
            self.response.headers['X-Arrow-Weaksum'] = str(chunk.weaksum)
            self.response.content_type = 'application/octet-stream'
            self.response.out.write(chunk.data)
        else:
            self.response.status = 404
            self.response.out.write("Chunk %s not found" % id)

    # Puts a new chunk.
    def put(self):
        id = getid(self.request.path)
        chunk = self.get_chunk(id)
        if chunk is not None:
            chunk.refcount = chunk.refcount + 1
        else:
            parity = None
            try:
                parity = int(self.request.headers['X-Arrow-Parity'])
            except:
                pass
            chunk = Chunk(id=id, refcount=1,
                          data=self.request.body_file.read(),
                          parity=parity)
        chunk.put()

    # Addref a chunk.
    def post(self):
        id = getid(self.request.path)
        chunk = self.get_chunk(id)
        if chunk is None:
            self.response.status = 404
            self.response.out.write("Chunk %s not found" % id)
            return
        if self.request.POST["addref"] == "1":
            chunk.refcount = chunk.refcount + 1
            chunk.put()

    # Deref a chunk.
    def delete(self):
        id = getid(self.request.path)
        chunk = self.get_chunk(id)
        if chunk is None:
            self.response.status = 404
            self.response.out.write("Chunk %s not found" % id)
            return
        chunk.refcount = chunk.refcount - 1
        if chunk.refcount == 0:
            chunk.delete()
        else:
            chunk.put()

def main():
    application = webapp.WSGIApplication([('/store/[0-9a-fA-F]+', StoreHandler)],
                                         debug=True)
    wsgiref.handlers.CGIHandler().run(application)
    
if __name__ == '__main__':
    main()