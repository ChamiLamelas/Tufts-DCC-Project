var=144
for i in $( eval echo {0..$var} ); do wget -c --retry-connrefused --tries=0 --timeout=50 http://alitrip.oss-cn-zhangjiakou.aliyuncs.com/TraceData/MSCallGraph/MSCallGraph_${i}.tar.gz; done
for i in $( eval echo {0..$var} ); do mv MSCallGraph_${i}.tar.gz ../data; done
for i in $( eval echo {0..$var} ); do tar -xf ../data/MSCallGraph_${i}.tar.gz -C ../data; done

