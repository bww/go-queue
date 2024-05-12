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
export PUBSUB_EMULATOR_HOST=${PUBSUB_EMULATOR_HOST:-localhost:8085}

# warnings
if [ "$PUBSUB_EMULATOR_HOST" != "localhost:8085" ]; then
  echo "WARNING: You are not using the default endpoint for the PubSub"
  echo "emulator. If you see timeouts or other unexplained errors during"
  echo "test runs, you may want to use the default setting, which is:"
  echo
  echo "  PUBSUB_EMULATOR_HOST=localhost:8085"
  echo
  echo "If you know what you're doing, you can ignore this warning."
  echo
fi

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
