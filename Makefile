APP = wc
SERVER_ADDR = 127.0.0.1:8080

build:
	cargo build --release

build-dylib-app:
	cd apps && rustc --crate-type dylib ${APP}.rs

sequential-run: clean-sequential-data build-dylib-app
	cargo run --release --bin sequential -- --app apps/lib${APP}.dylib --output-path results/${APP}/sequential --input inputs/*

distributed-run:
	make start-coordinator &
	sleep 1
	make start-workers
	sleep 1
	make merge

start-coordinator: clean-distributed-data
	cargo run --release --bin mrcoordinator -- --server-addr ${SERVER_ADDR} --output-path results/${APP}/distributed --input inputs/*

start-workers: build-dylib-app
	make start-worker &
	make start-worker &
	make start-worker

start-worker:
	cargo run --release --bin mrworker -- --app apps/lib${APP}.dylib --server-url http://${SERVER_ADDR}

clean-distributed-data:
	rm -rf results/${APP}/distributed

clean-sequential-data:
	rm -rf results/${APP}/sequential

merge:
	cd results/${APP}/distributed/output && sort mr-out* | grep . > mr-all

compare-output:
	diff results/${APP}/sequential/mr-out-0 results/${APP}/distributed/output/mr-all
