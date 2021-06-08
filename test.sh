#!/usr/bin/env bash

set -eo pipefail

# where am i?
me="$0"
me_home=$(dirname "$0")
me_home=$(cd "$me_home" && pwd)

# deps
DLV=dlv

# env
export PUBSUB_PROJECT_ID=pubsub-tests

# parse arguments
args=$(getopt dcv $*)
set -- $args
for i; do
  case "$i"
  in
    -d)
      debug="true";
      shift;;
    -c)
      other_flags="$other_flags -cover";
      shift;;
    -v)
      other_flags="$other_flags -v";
      shift;;
    --)
      shift; break;;
  esac
done

if [ ! -z "$debug" ]; then
  "$DLV" test $* -- $other_flags
else
  go test $other_flags $*
fi
