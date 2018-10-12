FROM continuumio/miniconda3

MAINTAINER Benjamin Stewart <ben.gis.stewart@gmail.com>

VOLUME /data/input
VOLUME /data/output
VOLUME /build3rd
VOLUME /require

# Install Linux build files for compiling Cython extensions
RUN apt-get update -y

# Create a virtual environment
RUN conda update -n base conda
RUN conda create --name spfeasenv python=2.7 -y

RUN apt-get install python-dev apt-utils libc-dev linux-headers-amd64 gcc -y

RUN . ~/.bashrc

# Add Conda bin to the path
ENV PATH /usr/bin:/usr/local/bin:/root/anaconda2/bin:/opt/conda/bin:/opt/conda/envs/spfeasenv/bin:$PATH
ENV PYTHONPATH /root/anaconda2/envs/spfeasenv/lib/python2.7/site-packages:/opt/conda/envs/spfeasenv/lib/python2.7/site-packages:$PYTHONPATH

# Force to Python 2.7
RUN conda install python=2.7 -y

# Activate the virtual environment and install Anaconda libraries
RUN ["/bin/bash", "-c", "source activate spfeasenv && conda install --name spfeasenv cython numpy scipy numexpr matplotlib scikit-image scikit-learn bottleneck pytables pandas geopandas rasterio opencv statsmodels pyyaml joblib psutil beautifulsoup4 colorama six decorator subprocess32 gdal -y"] 

ADD /gbdx-task-interface/ /build3rd/gbdx-task-interface/

# Install gbdx-task-interface
WORKDIR /build3rd/gbdx-task-interface/
RUN ["/bin/bash", "-c", "source activate spfeasenv && python setup.py build && python setup.py install"]

WORKDIR /root

# Copy python scripts for running analysis
COPY ./run_spfeas_Zonal.py .
RUN chmod +x run_spfeas_Zonal.py
