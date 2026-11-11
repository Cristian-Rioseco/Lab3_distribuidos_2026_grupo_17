.PHONY: docker-VM1 docker-VM2 docker-VM3 docker-VM4 down-VM1 down-VM2 down-VM3 down-VM4

docker-VM1:
	docker compose -f docker-compose-mv1.yml up -d --build

docker-VM2:
	docker compose -f docker-compose-mv2.yml up -d --build

docker-VM3:
	docker compose -f docker-compose-mv3.yml up -d --build

docker-VM4:
	docker compose -f docker-compose-mv4.yml up -d --build

down-VM1:
	docker compose -f docker-compose-mv1.yml down -v

down-VM2:
	docker compose -f docker-compose-mv2.yml down -v

down-VM3:
	docker compose -f docker-compose-mv3.yml down -v

down-VM4:
	docker compose -f docker-compose-mv4.yml down -v