#!/bin/bash

# Modify this line for MacOS
export PATH="/lib/postgresql/14/bin:${PATH}"
export PGPORT=8889
export PGHOST=/tmp
initdb $HOME/trendr
pg_ctl -D $HOME/trendr -o '-k /tmp' start
createdb trendr

