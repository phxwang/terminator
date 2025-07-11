#!/bin/bash

cargo run -- --log-file logs/crank.log crank 1> /dev/null 2>&1 &
sleep 5
cargo run -- --log-file logs/stream_liquidate_big.log stream-liquidate --scope big_near_liquidatable_obligations 1> logs/stream_liquidate_big_near_output.log 2>&1 &
sleep 5
cargo run -- --log-file logs/stream_liquidate_medium.log stream-liquidate --scope medium_near_liquidatable_obligations 1> logs/stream_liquidate_medium_near_output.log 2>&1 &
sleep 5
cargo run -- --log-file logs/stream_liquidate_small.log stream-liquidate --scope small_near_liquidatable_obligations 1> logs/stream_liquidate_small_near_output.log 2>&1 &
# tail -f logs/stream_liquidate_big_fish.log | grep 'Liquidating'