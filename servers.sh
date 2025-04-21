./dfs dfs1 10001 & PID1=$!
./dfs dfs2 10002 & PID2=$!
./dfs dfs3 10003 & PID3=$!
./dfs dfs4 10004 & PID4=$!
echo "Servers Started!"
echo "$PID1 $PID2 $PID3 $PID4" > dfs_pids.txt

./dfs dfs1 10001 &
./dfs dfs2 10002 &
./dfs dfs3 10003 &
./dfs dfs4 10004 &
./dfs dfs5 10005 &
