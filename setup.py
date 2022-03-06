import pathlib
from setuptools import setup

# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()

# This call to setup() does all the work
setup(
    name="ut_msba_edgar_scraper",
    version="1.4.0",
    description="UT MSBA Edgar Scraper",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/Brandt-moreThan4/sec-edgar-downloader",
    author="The Meta Team",
    author_email="153144green@chsbr.net",
    license="MIT",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
    ],
    packages=["ut_msba_edgar_scraper"],
    # package_data={'ut_msba_edgar_scraper': ['/*.csv']},
    include_package_data=True,
    install_requires=["pandas",'bs4'],
)