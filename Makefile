help: 
	@echo "		exec	 Exec container airflow"
	@echo "		stop     Stops the docker containers"
	@echo "		up     	 Runs the whole containers, served under https://localhost:8080/"

exec:
	docker exec -it airflow_british_proj-airflow-webserver-1 bash

stop:
	docker compose down
	docker compose stop

up:
	docker compose up -d