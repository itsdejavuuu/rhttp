$ErrorActionPreference = "Stop"

cargo +nightly build --release --target x86_64-pc-windows-msvc
New-Item -ItemType Directory -Force -Path dist | Out-Null
Copy-Item target/x86_64-pc-windows-msvc/release/gmsv_rhttp.dll dist/gmsv_rhttp_win64.dll
