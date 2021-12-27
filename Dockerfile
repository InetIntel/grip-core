FROM rackspacedot/python38:latest
LABEL maintainer="Mingwei Zhang <mingwei@caida.org>"

WORKDIR /src
COPY ./setup.py ./
COPY ./MANIFEST.in ./
COPY grip ./grip
RUN python3 -m pip install .
RUN python3 setup.py install

CMD echo "run specific commands instead of default one"
