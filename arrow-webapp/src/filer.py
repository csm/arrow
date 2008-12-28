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
import fileinfo
    
class FileHandler(webapp.RequestHandler):
    def get(self):
        file_id = getid(self.request.path)
        file = File.gql("WHERE name = :1", file_id)
        if file is not None:
            if file.type == FileType:
                self.response.headers['X-Arrow-File-Type'] = 'file'
                self.response.headers['X-Arrow-MD5'] = file.hash
                if file.previous is not None:
                    self.response.headers['X-Arrow-Previous-ID'] = file.previous
                self.response.headers['X-Arrow-File-Size'] = str(file.size)
                self.response.headers['X-Arrow-File-Mode'] = str(file.mode)
                self.response.headers['X-Arrow-File-Chunk-Size'] = str(file.chunk_size)
                self.response.headers['X-Arrow-File-Mtime'] = str(file.mtime)
                self.response.hedaers['X-Arrow-File-Ctime'] = str(file.ctime)
            else:
                self.response.headers['X-Arrow-File-Type'] = 'directory'
                self.response.headers['X-Arrow-File-Mode'] = str(file.mode)
                self.response.headers['X-Arrow-File-Mtime'] = str(file.mtime)
                self.response.hedaers['X-Arrow-File-Ctime'] = str(file.ctime)
            for chunk in file.chunks:
                self.response.out.print(chunk)
        else:
            self.result.status = 404

    def head(self):
        file_id = getid(self.request.path)
        file = File.gql("WHERE name = :1", file_id)
        try:
            if file is None:
                raise Exception('no such file')
            if file.type == FileType:
                self.response.headers['X-Arrow-File-Type'] = 'file'
                self.response.headers['X-Arrow-MD5'] = file.hash
                if file.previous is not None:
                    self.response.headers['X-Arrow-Previous-ID'] = file.previous
                self.response.headers['X-Arrow-File-Size'] = str(file.size)
                self.response.headers['X-Arrow-File-Mode'] = str(file.mode)
                self.response.headers['X-Arrow-File-Chunk-Size'] = str(file.chunk_size)
                self.response.headers['X-Arrow-File-Mtime'] = str(file.mtime)
                self.response.hedaers['X-Arrow-File-Ctime'] = str(file.ctime)
            elif file.type == DirectoryType:
                self.response.headers['X-Arrow-File-Type'] = 'directory'
                self.response.headers['X-Arrow-File-Mode'] = str(file.mode)
                self.response.headers['X-Arrow-File-Mtime'] = str(file.mtime)
                self.response.hedaers['X-Arrow-File-Ctime'] = str(file.ctime)
            else:
                raise Exception('invalid file type from store!')
        except:
            self.result.status = 404
            
    def put(self):
        try:
            file_id = getid(self.request.path)
            file = File.gql('WHERE name = :1', file_id)
            file_type_string = self.response.headers['X-Arrow-File-Type']
            file_type = 0
            if file_type_string == 'file':
                file_type = FileType
            elif file_type_string == 'directory':
                file_type = DirectoryType
            else:
                raise Exception("invalid file type '%s'" % file_type_string)
            file_mode = int(self.request.headers['X-Arrow-File-Type'])
            file_mtime = int(self.request.headers['X-Arrow-File-Mtime'])
            file_ctime = int(self.request.headers['X-Arrow-File-Ctime'])
            prev_id = None
            if file_type == FileType:
                if 'X-Arrow-Previous-ID' in self.request.keys():
                    prev_id = self.request.headers['X-Arrow-Previous-ID']
            
            new_file = File(name = file_id)
        except Exception, msg:
            self.result.status = 500
            self.result.out.write(str(msg))

def main():
    application = webapp.WSGIApplication([('/files/[0-9a-fA-F]+', FileHandler)],
                                         debug=True)
    wsgiref.handlers.CGIHandler().run(application)
    
if __name__ == '__main__':
    main()