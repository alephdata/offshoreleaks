

all: \
	data/bahamas_leaks.ijson \
	data/offshore_leaks.ijson \
	data/panama_papers.isjon \
	data/paradise_papers.ijson

data:
	mkdir -p data/

data/bahamas_leaks.zip: data
	wget -q -c -O data/bahamas_leaks.zip https://offshoreleaks-data.icij.org/offshoreleaks/csv/csv_bahamas_leaks.2017-12-19.zip

data/offshore_leaks.zip: data
	wget -q -c -O data/offshore_leaks.zip https://offshoreleaks-data.icij.org/offshoreleaks/csv/csv_offshore_leaks.2018-02-14.zip

data/panama_papers.zip: data
	wget -q -c -O data/panama_papers.zip https://offshoreleaks-data.icij.org/offshoreleaks/csv/csv_panama_papers.2018-02-14.zip

data/paradise_papers.zip: data
	wget -q -c -O data/paradise_papers.zip https://offshoreleaks-data.icij.org/offshoreleaks/csv/csv_paradise_papers.2018-02-14.zip

%.sqlite: %.zip
	python make_db.py $*.zip $@

%.ijson: %.sqlite
	python make_entities.py $*.sqlite $@