#!/bin/bash

cargo run -- --log-file logs/crank.log crank 1> /dev/null 2>&1 &
sleep 10
cargo run -- --log-file logs/loop_liquidate_big_fish.log loop-liquidate --scope big_fish_near_liquidatable_obligations 1> logs/loop_liquidate_big_fish_output.log 2>&1 &
sleep 10
cargo run -- --log-file logs/loop_liquidate_near.log loop-liquidate --scope near_liquidatable_obligations 1> logs/loop_liquidate_near_output.log 2>&1 &
tail -f logs/loop_liquidate_big_fish.log | grep 'Liquidating'