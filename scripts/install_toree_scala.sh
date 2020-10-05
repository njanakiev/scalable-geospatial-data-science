jupyter toree install \
  --user \
  --debug \
  --log-level=DEBUG \
  --replace \
  --spark_home=/usr/local/spark/ \
  --spark_opts='--master=local[4]' \
  --intepreters=Scala,SQL
