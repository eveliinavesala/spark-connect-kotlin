.PHONY: build-docker
build-docker:
	docker build -t spark .

.PHONY: run-docker
run-docker:
    docker run -p 15002:15002 spark