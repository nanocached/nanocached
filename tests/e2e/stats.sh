#!/bin/bash
out=$1; prefix=$2; iv=${3:-10}
echo "ts,name,cpu,mem_usage,mem_bytes,net_io,pids" > "$out"
while true; do
  ts=$(date +%s)
  docker stats --no-stream --format '{{.Name}},{{.CPUPerc}},{{.MemUsage}},{{.NetIO}},{{.PIDs}}' 2>/dev/null \
   | grep "^$prefix" | while IFS=, read -r name cpu mem net pids; do
      raw=${mem%% /*}; num=${raw%[KMG]iB}; unit=${raw#"$num"}
      case $unit in KiB) b=$(echo "$num*1024"|bc);; MiB) b=$(echo "$num*1048576"|bc);; GiB) b=$(echo "$num*1073741824"|bc);; *) b=${raw%B};; esac
      echo "$ts,$name,$cpu,$raw,${b%.*},$net,$pids" >> "$out"
  done
  sleep "$iv"
done
