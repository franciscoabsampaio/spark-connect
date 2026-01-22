docker:
	docker run --name spark-delta -p 15002:15002 -d franciscoabsampaio/spark-connect-server:delta

stop:
	docker stop spark-delta && docker rm spark-delta