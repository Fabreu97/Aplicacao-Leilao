#!/bin/bash
@echo "Donwloading all Branches . . ."

for branch in $(git branch -r | grep -v '\->' | sed 's/ *origin\///'); do
    git checkout $branch
done
