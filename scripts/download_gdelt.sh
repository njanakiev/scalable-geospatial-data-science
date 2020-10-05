# real    67m35.970s
# user    0m30.267s
# sys     5m16.946s

#while IFS=',' read -r name url
#do
#  if [[ "$name" == *.zip ]]; then
#    echo "$name"
#    wget "$url" -O "../data/raw/$name"
#  fi
#done < ../data/gdelt_urls.csv

#for day in {01..31}; do
#  URL="http://data.gdeltproject.org/events/202006${day}.export.CSV.zip"
#  echo $URL
#  wget $URL -P ../data/raw
#done
