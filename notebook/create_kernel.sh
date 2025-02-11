#!/bin/bash
# Kill any existing kernel using the same ports
pkill -f "spyder_kernels.console --ip=127.0.0.1 --hb=12345"
# Start the kernel with fixed parameters
python -m spyder_kernels.console \
    --ip=127.0.0.1 \
    --hb=12345 \
    --shell=54321 \
    --iopub=12346 \
    --stdin=12347 \
    --control=12348 \
    --key="your-secret-hmac-key"