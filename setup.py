from setuptools import setup, find_packages

setup(
    name="GOST_GBDX_Tools",
    version="0.1",
    packages=find_packages(),

    # Project uses reStructuredText, so ensure that the docutils get
    # installed or upgraded on the target machine
    install_requires=[#'docutils>=0.3',
                      'dask>=1.2.2',
                      'geopandas>=0.4.0',
                      'gbdxtools>=0.15.13',
                      'pandas>=0.23.5',
                      'requests==2.19.1',
                      'shapely>=1.6.4'
                      ],

    package_data={
        # If any package contains *.txt or *.rst files, include them:
        # And include any *.msg files found in the 'hello' package, too:
    },

    # metadata to display on PyPI
    author="Benjamin P. Stewart",
    author_email="ben.gis.stewart@gmail.com",
    description="Wrapper around GBDx tools to simplify launching and monitoring tasks",
    license="PSF",
    keywords="Imagery satellite gbdx DigitalGlobe",
    url="https://github.com/worldbank/GOST_GBDX",   # project home page, if any
    project_urls={
        "Bug Tracker": "https://github.com/worldbank/GOST_GBDX/issues",
        "Documentation": "TBD",
        "Source Code": "https://github.com/worldbank/GOST_GBDX",
    }

    # could also include long_description, download_url, classifiers, etc.
)