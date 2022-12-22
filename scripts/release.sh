#!/usr/bin/env bash

if [[ $(git status --porcelain) ]]; then
  echo "error: can't release if there are uncommitted changes"
  exit 1
fi

# TODO
echo "TODO"
