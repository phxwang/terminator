#!/bin/bash

cargo run -- --log-file logs/crank.log crank 1> /dev/null 2>&1 &
tail -f logs/crank.log | grep 'Liquidating'