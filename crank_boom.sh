#!/bin/bash

cargo run -- crank >> logs/crank.log 2>&1 &
tail -f crank.log | grep 'Liquidating'