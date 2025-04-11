start-jupyter:
	WORKDIR=$(pwd); docker run -it --rm \
		-p 8888:8888 \
		-p 4040:4040 \
		-v "$WORKDIR":/home/jovyan/work \
		jupyter/pyspark-notebook
