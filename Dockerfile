FROM akariv/dgp-app:036980dc8463956beedf5b502717ff30e6440b62

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
