base_url="http://data.gdeltproject.org/events"
dst_folderpath="../data/raw"
year="2020"


if [[ "$1" = "download" ]]; then
  for month in {01..12}; do
    for day in {01..31}; do
      url="${base_url}/2020${month}${day}.export.CSV.zip"
      zip_filepath="${dst_folderpath}/${year}${month}${day}.export.CSV.zip"
    
      echo "Downloading ${url} ..."
      wget $url -P $dst_folderpath
    
      if [[ -f $zip_filepath ]]; then
        echo $zip_filepath
        unzip $zip_filepath -d $dst_folderpath
      fi
    done
  done
fi

if [[ "$1" = "lowercase" ]]; then
  for filepath in $dst_folderpath/*; do 
    dst_filepath=$(echo $filepath | tr "[:upper:]" "[:lower:]")
    if [[ ! -f $dst_filepath ]]; then
      echo $filepath $dst_filepath
      mv $filepath $dst_filepath
    fi
  done
fi
