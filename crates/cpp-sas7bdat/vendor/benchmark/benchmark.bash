#!/bin/bash

hyperfine --warmup 1 \
	  "../build/Release/apps/cppsas7bdat-ci null $1" \
	  "python3 ./cppsas7bdat.py -f $1 -s sink" \
	  "python3 ./cppsas7bdat.py -f $1 -s chunk" \
	  "python3 ./cppsas7bdat.py -f $1 -s pd_sink" \
	  "python3 ./cppsas7bdat.py -f $1 -s pd_chunk" \
	  "python3 ./cppsas7bdat.py -f $1 -s pd_data" \
	  "readstat -f $1 /tmp/a.csv" \
	  "python3 ./pypandas.py $1" \

	  #"python3 ./pysas7bdat.py $1" \
	  #"julia -e 'using SASLib; readsas(ARGS[1]);' $1" \

Rscript benchmark.R $1

julia -e 'using BenchmarkTools; using SASLib; @btime redirect_stdout(open(tempname(), "w")) do; readsas(ARGS[1]) end;' $1
