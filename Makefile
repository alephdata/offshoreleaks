

all: \
	data/bahamas_leaks.ijson \
	data/offshore_leaks.ijson \
	data/panama_papers.ijson \
	data/paradise_papers.ijson

sqlite: \
	data/bahamas_leaks.sqlite \
	data/offshore_leaks.sqlite \
	data/panama_papers.sqlite \
	data/paradise_papers.sqlite

data/bahamas_leaks.zip:
	mkdir -p data/
	wget -q -c -O data/bahamas_leaks.zip https://offshoreleaks-data.icij.org/offshoreleaks/csv/csv_bahamas_leaks.2017-12-19.zip

data/offshore_leaks.zip:
	mkdir -p data/
	wget -q -c -O data/offshore_leaks.zip https://offshoreleaks-data.icij.org/offshoreleaks/csv/csv_offshore_leaks.2018-02-14.zip

data/panama_papers.zip:
	mkdir -p data/
	wget -q -c -O data/panama_papers.zip https://offshoreleaks-data.icij.org/offshoreleaks/csv/csv_panama_papers.2018-02-14.zip

data/paradise_papers.zip:
	mkdir -p data/
	wget -q -c -O data/paradise_papers.zip https://offshoreleaks-data.icij.org/offshoreleaks/csv/csv_paradise_papers.2018-02-14.zip

%.sqlite: %.zip
	python make_db.py $*.zip $@

%.ijson: %.sqlite
	python make_entities.py $*.sqlite $@