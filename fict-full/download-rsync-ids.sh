#!/bin/bash

rsync_ids="${1:-rsync-ids.txt}"

rsync -av --files-from="$rsync_ids" data.analytics.hathitrust.org::features/ json-files/.


