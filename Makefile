full: data/full-oldb.zip \
	data/full-oldb.ijson \
	data/full-oldb.sqlite

data/full-oldb.zip:
	mkdir -p data/
	wget -q -c -O data/full-oldb.zip https://offshoreleaks-data.icij.org/offshoreleaks/csv/full-oldb-20211202.zip

%.sqlite: %.zip
	python make_db.py $*.zip $@

%.ijson: %.sqlite
	python make_entities.py $*.sqlite $@

clean-entities:
	rm -f data/*.ijson

clean: clean-entities
	rm -rf data/*.sqlite