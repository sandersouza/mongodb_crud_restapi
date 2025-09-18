#!/usr/bin/env bash
BYTES=32
TOKEN=$(openssl rand -hex $BYTES)
echo "$TOKEN"
