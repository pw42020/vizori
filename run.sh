#!/usr/bin/env bash
PYTHONPATH=$(pwd)/src/

poetry env activate
uvicorn app.main:app --reload