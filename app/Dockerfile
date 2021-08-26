# Define global args

FROM continuumio/miniconda3 AS conda-build

#  Because miniconda3 image releases may sometimes be behind we need to play catchup manually
# RUN conda update conda && conda install "conda=4.8.2"
# RUN conda update conda

ENV CONDA_ENV_NAME conda-env

# Lets get the environment up to date
RUN apt clean 
RUN apt-get update
RUN apt-get install -y \
  libudunits2-dev \ 
  libproj-dev \
  proj-bin \
  libgeos-dev \
  libcunit1-dev 
  # musl-dev 
  # --no-install-recommends


  WORKDIR /usr/src/app
COPY environment.yml .

# Now we want to activate a Conda environment which has the necessary Python version installed and has all the libraries installed required to run our app
RUN conda env create -n $CONDA_ENV_NAME -f environment.yml
RUN echo "source activate $CONDA_ENV_NAME" > /etc/bashrc
ENV PATH=/opt/conda/envs/$CONDA_ENV_NAME/bin:$PATH

COPY *.py .


ENTRYPOINT [ "python3", "-m", "awslambdaric" ]
CMD [ "process_data.lambda_handler" ] 
