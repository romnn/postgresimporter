from distutils.core import setup
from pathlib import Path

short_description = (
    "A simple python wrapper script based on pgfutter "
    "to load multiple dumped csv files into a postgres database."
)

try:
    import m2r

    long_description = m2r.parse_from_file(Path().parent / "README.md")
except ImportError:
    long_description = short_description

setup(
    name="postgresimporter",
    packages=["postgresimporter"],
    version="0.1.3",
    license="MIT",
    description=short_description,
    long_description=long_description,
    author="romnn",
    author_email="contact@romnn.com",
    url="https://github.com/romnnn/postgresimporter",
    keywords=["postgres", "PostgreSQL", "database", "import", "load", "dump", "CSV"],
    python_requires=">=3.6",
    install_requires=[
        "jinja2",
        "dataclasses",
        "progressbar2",
        "chardet",
        "prettytable",
    ],
    extras_require=dict(dev=["blessings", "pygments", "m2r", "pyfakefs"]),
    package_data={"postgresimporter": ["hooks"]},
    classifiers=[
        "Environment :: Console",
        "Operating System :: OS Independent",
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Build Tools",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    entry_points={"console_scripts": ["postgresimporter=postgresimporter:main"]},
)
