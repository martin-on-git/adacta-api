#!/bin/bash
set -e

rsync -avz --delete \
  --exclude '.git' \
  --exclude '__pycache__' \
  --exclude '.DS_Store' \
  --exclude 'sync-to-sparc.sh' \
  "$PWD/" \
  pi@sparc.local:/opt/adacta-api/
