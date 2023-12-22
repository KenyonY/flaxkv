.PHONY: start build exec down log rm

image := "beidongjiedeguang/flaxkv:latest"
container := "flaxkv-container"
compose_path := "docker-compose.yaml"


start:
	@docker run -d \
	--restart=unless-stopped \
    --name $(container) \
    -p 8000:8000 \
    $(image) --port=8000
	@make log


exec:
	docker exec -it $(container) sh

log:
	docker logs -f $(container)

rm:
	docker rm -f $(container)

build:
	docker build --tag $(image) .