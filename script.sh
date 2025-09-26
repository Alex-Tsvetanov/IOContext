MonitorBenchmark() {
    ( while true; do
        echo "=== $(date +'%F %T') ==="
        ss -tan "sport = $1" | awk 'NR>1 {print $1}' | sort | uniq -c
        sleep 1
    done >> ss_log_$2.txt
    ) &

    LOGGER_PID=$!

    # Run wrk
    wrk -t16 -c4096 -d30s http://127.0.0.1$1 > /tmp/$2_data;

    # Kill logger after wrk finishes
    kill $LOGGER_PID

    cat /tmp/$2_data | grep "requests in 30" >> $2_numbers;
    cat /tmp/$2_data | grep "Requests/sec:" >> $2_numbers;

}

for i in `seq 1 5` ; do
    echo "Run $i";
    echo "io_uring";
    MonitorBenchmark ":8081" io_uring;
    
    echo "epoll";
    MonitorBenchmark ":8082" epoll;
done

cmake --build build --config Release && \
sudo systemctl restart myserver && \
sudo strace -f \
    -e trace=io_uring_enter,io_uring_setup,io_uring_register,accept4,close,shutdown,setsockopt,getsockopt \
    -p `sudo systemctl status myserver | grep Main\ PID: | cut -d' ' -f6` -o strace_output.log