@echo off
set IP1=127.0.0.1
set IP2=127.0.0.2
set IP3=127.0.0.3
set IP4=127.0.0.4
set IP5=127.0.0.5

REM Run each IP instance in a new Command Prompt window
start cmd /k "cargo run -- %IP1%"
timeout /t 2 > nul
start cmd /k "cargo run -- %IP2%"
timeout /t 2 > nul
start cmd /k "cargo run -- %IP3%"
timeout /t 2 > nul
start cmd /k "cargo run -- %IP4%"
timeout /t 2 > nul
start cmd /k "cargo run -- %IP5%"