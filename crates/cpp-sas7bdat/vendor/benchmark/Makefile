.PHONY: benchmark
benchmark:
	./benchmark.bash ../test/data_AHS2013/topical.sas7bdat
	./benchmark.bash ../test/data_misc/numeric_1000000_2.sas7bdat
	./benchmark.bash ../test/data_gov/pss1718_pu.sas7bdat

.PHONY: install
install:
	#cd ~/dev; git clone https://github.com/sharkdp/hyperfine.git;
	wget https://github.com/sharkdp/hyperfine/releases/download/v1.12.0/hyperfine_1.12.0_amd64.deb
	sudo dpkg -i hyperfine_1.12.0_amd64.deb

