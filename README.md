# streaming-project-spark-postgres
** chạy chương trình sử dụng thư viện ngoài chạy spark submit**
'''
docker container stop test-spark ;
docker container rm test-spark ;
docker run -ti --name test-spark `
--network=streaming-network `
-p 4040:4040 `
-v "${PWD}:/spark" `
-v "${PWD}/checkpoints:/tmp/spark-checkpoints" `
-v spark_lib:/opt/bitnami/spark/.ivy2 `
-v spark_data:/data `
-e PYSPARK_DRIVER_PYTHON=python `
-e PYSPARK_PYTHON=./environment/bin/python `
unigap/spark:3.5 bash -c "python -m venv pyspark_venv && \
source pyspark_venv/bin/activate && \
pip install --no-cache-dir -r /spark/requirements.txt && \
venv-pack -o pyspark_venv.tar.gz && \
spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
--archives pyspark_venv.tar.gz#environment \
--py-files /spark/postgres_database.py \
/spark/streaming_process.py"
'''