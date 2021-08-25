# Define global args

FROM continuumio/miniconda3 AS conda-build

COPY environment.yml .
RUN conda env create -f environment.yml

# Install conda-pack:
RUN conda install -c conda-forge conda-pack
# conda search python
RUN conda install pip

RUN pip install awslambdaric boto3

RUN mkdir -p "/app"

COPY process_data.py "/app"

WORKDIR "/app"

ENTRYPOINT [ "/opt/conda/bin/python", "-m", "awslambdaric" ]
CMD [ "process_data.lambda_handler" ] 
