full: data/full-oldb.ijson

data/full-oldb.zip:
	mkdir -p data/
	wget -q -c -O data/full-oldb.zip https://offshoreleaks-data.icij.org/offshoreleaks/csv/full-oldb-20211202.zip

data/full-oldb.ijson: data/full-oldb.zip
	python oldb_to_ftm.py data/full-oldb.zip data/full-oldb.ijson

clean-entities:
	rm -f data/full-oldb.ijson

clean: clean-entities
	rm -rf data/full-oldb.zip