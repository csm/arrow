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


from google.appengine.ext import db

class Chunk(db.Model):
    # The chunk identifier; the hex hash of the chunk.
    id = db.StringProperty(required = True)
    
    # The weak checksum of the chunk.
    weaksum = db.IntegerProperty(required = True)
    
    # The chunk data.
    data = db.BlobProperty(required = True)
    
    # The chunk parity (optional; we aren't computing this now)
    parity = db.BlobProperty(required = False)
    
    # The reference count to this chunk.
    refcount = db.IntegerProperty(required = True)

