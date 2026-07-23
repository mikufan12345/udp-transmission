if [ ! -d "./build" ]; then
	# make build directory
	mkdir build
fi
cd build && cmake .. && make
