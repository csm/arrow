# fileinfo.py -- file objects
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

from google.appengine.ext import db

FileType = 1
DirectoryType = 2

class File(db.Model):
    # File type.
    type = db.IntProperty(required = True)

    # The file name.
    name = db.StringProperty(required = True)

    # The MD5 sum of the file.
    hash = db.StringProperty(required = True)

    # The ID of the previous version of this file.
    previous = db.StringProperty(required = False)

    # The size of the file, in bytes
    size = db.IntProperty(required = True)

    # The file's UNIX mode.
    mode = db.IntProperty(required = True)

    # The chunk size, in bytes.
    chunk_size = db.IntProperty(required = True)

    # The file's modification time.
    mtime = db.IntProperty(required = True)

    # The file's status change time.
    ctime = db.IntProperty(required = True)

    # The list of chunk identifiers, or files.
    chunks = db.ListProperty(required = True)
