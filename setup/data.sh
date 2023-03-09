mkdir -p ../data
start=11
end=144
for i in $( eval echo {$start..$end} )
do
	wget -c --retry-connrefused --tries=0 --timeout=50 http://alitrip.oss-cn-zhangjiakou.aliyuncs.com/TraceData/MSCallGraph/MSCallGraph_${i}.tar.gz
	tar -xf MSCallGraph_${i}.tar.gz -C ../data
	rm MSCallGraph_${i}.tar.gz
done


