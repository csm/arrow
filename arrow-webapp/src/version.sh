#!/usr/bin/bash

SVN=/opt/subversion/bin/svn
REVISION="?"

x=`${SVN} info | grep Revision: | cut -f 1 -d ' '`

if [ "$x" ]; then REVISION=$x; done

sed "s/@@SVN_REVISION@@/$REVISION/" < app.yaml.in > app.yaml
