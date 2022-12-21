#!/bin/bash

# Copyright 2022 The Parca Authors
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# post-checkout hook - looks for changes to package.json,
# when you change branches, and if found, reinstalls the packages

# Exit early if this was only a file checkout, not a branch change ($3 == 1)
[[ $3 == 0 ]] && exit 0

oldRef=$1
newRef=$2

# Exit early if the local branch is behind the remote
LOCAL=$(git rev-parse @)
REMOTE=$(git rev-parse @'{u}' 2>/dev/null)
BASE=$(git merge-base @ @'{u}' 2>/dev/null)

if [[ "$LOCAL" != "$REMOTE" && "$LOCAL" = "$BASE" ]]; then
    echo "You are behind origin, not running post-checkout hook."
    exit 1
fi

function changed {
    git diff --name-only "$oldRef" "$newRef" | grep "^$1" >/dev/null 2>&1
}

if changed 'package.json'; then
    echo "Package.json changed, removing npm dependencies and re-building"

    find . -name 'node_modules' -type d -prune -exec rm -rf '{}' +
    find . -name 'dist' -type d -prune -exec rm -rf '{}' +

    cd ui && yarn install && yarn build
    exit 0
fi
