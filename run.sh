#!/bin/sh

set -e

dfx deploy
dfx canister call prog_backend main
