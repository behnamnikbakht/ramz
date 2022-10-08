ARG FUNCTION_DIR="/home/app/"

FROM python:3.10

ARG FUNCTION_DIR
RUN mkdir -p ${FUNCTION_DIR}
COPY requirements.txt ${FUNCTION_DIR}

RUN pip install -r ${FUNCTION_DIR}requirements.txt --target ${FUNCTION_DIR}

ARG FUNCTION_DIR
COPY . ${FUNCTION_DIR}

WORKDIR ${FUNCTION_DIR}

#CMD [ "python", "-m", "gatherer" ]
ENTRYPOINT [ "python", "./gatherer.py" ]