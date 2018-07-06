rm src/resources/artists_small/data.tsv
rm src/resources/titles_small/data.tsv
rm src/resources/artist.title_small/data.tsv
echo "VALOR =   $1"
head -n $1 src/resources/artists/artist.tsv  > src/resources/artists_small/data.tsv
head -n $1 src/resources/titles/titles.tsv > src/resources/titles_small/data.tsv
head -n $1 src/resources/artist.title/artist.title.tsv > src/resources/artist.title_small/data.tsv
