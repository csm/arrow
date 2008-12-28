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

def isid(string):
    """
    Tell if the argument is a 32-character hex string.
    """
    if (len(string) != 32):
        return False
    for char in string:
        if char.isdigit() or (char >= 'a' and char <= 'f'):
            pass
        else:
            return False
    return True

def getid(uri):
    """
    Fetch the ID portion of a URI -- this is the final component of
    the path, which must be a 32-character hex string.
    """
    comps = uri.split('/')
    id = comps[len(comps) - 1].lower()
    if id == '':
        id = comps[len(comps) - 2].lower()
    if not isid(id):
        raise Exception('not a valid ID: %s', id)
    return id
