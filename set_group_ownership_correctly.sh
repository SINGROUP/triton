#!/bin/bash
#
# Script for setting all of your files and directories to be owned by the group triton-users,
# which should fix the "disk quota exceeded" error that appears although your user-specific
# quota is fine.
#
# Eero Holmstrom, 2017 
#

lfs find $WRKDIR -type d --print0 | xargs -0 chmod g+s
lfs find /scratch/work/$(whoami)/ --print0 | xargs -0 chown :triton-users -v
lfs find ~/ --print0 | xargs -0 chown :triton-users -v

exit 0;
