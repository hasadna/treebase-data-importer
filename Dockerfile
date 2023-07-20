FROM akariv/dgp-app:0f16e296f4ede932bd6addbda1894aa4e009cbd2

USER root
RUN apt-get install -y wget unzip build-essential libsqlite3-dev zlib1g-dev libspatialindex6
RUN wget https://github.com/mapbox/tippecanoe/archive/refs/tags/1.36.0.zip && \
    unzip 1.36.0.zip && rm 1.36.0.zip && cd tippecanoe-1.36.0 && make -j && \
    make install && cd .. && rm -rf tippecanoe-1.36.0
USER etl

COPY requirements.dev.txt .
RUN sudo pip install -U -r requirements.dev.txt

COPY configuration.json dags/
COPY logo.png ui/dist/ui/he/assets/logo.png
COPY logo.png ui/dist/ui/en/assets/logo.png

COPY taxonomies taxonomies
COPY treebase treebase
COPY operators dags/operators/
COPY setup.py .

RUN pip install . 
