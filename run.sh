#!/usr/bin/env bash
# export PYTHONPATH=$(pwd)/src/

poetry env activate
uvicorn app.main:app --reload