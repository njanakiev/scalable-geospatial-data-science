sudo docker run -p 5050:80 \
    --name pgadmin4 \
    -e "PGADMIN_DEFAULT_EMAIL=${PGADMIN_USERNAME}" \
    -e "PGADMIN_DEFAULT_PASSWORD=${PGADMIN_PASSWORD}" \
    -d dpage/pgadmin4
