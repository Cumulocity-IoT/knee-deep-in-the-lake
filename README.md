# Knee-deep in the lake

This repository provides a technical training on data lake technologies for IoT data management. It focuses on practice by providing notebooks that you can actually run, modify and enhance yourself. Feel encouraged to experiment with your own data and own settings. Contributions and pull requests are highly welcome!

## Objectives

Learn practical skills for IoT data management and analytics:

- Apache Parquet file formats, encodings, and compression strategies
- Apache Iceberg table operations, schema evolution, and time travel
- Data lake ecosystem tools and integration patterns, and the medallion architecture
- Performance optimization techniques for large-scale data processing

## Modules

* Module 1: Apache Parquet
* Module 2: Apache Iceberg
* Module 3: Medallion architecture

## Prerequisites

The training is oriented towards people with some level of development background and technical interest. The following technology stack is used:

- **Python+** - Core programming language
- **Jupyter Lab** - Interactive development environment
- **Apache Parquet** - Columnar storage format
- **Apache Iceberg** - Table format for data lakes
- **PyArrow** - Fast columnar data processing
- **Pandas** - Data manipulation and analysis
- **DuckDB** - In-process analytical database
- **Plotly** - Interactive visualizations

You do not need to know the entire stack, but some level of Python and SQL knowledge is required. It is also helpful to have a basic understanding of [Jupyter Notebooks](https://jupyterlab.readthedocs.io/en/latest/). If you are a VS Code user, there is an [extension for notebooks](https://marketplace.visualstudio.com/items?itemName=ms-toolsai.jupyter) and for [Python](https://marketplace.visualstudio.com/items?itemName=ms-python.python).

Since different programming languages use different optimization heuristics when writing Parquet files, code for other languages is also being developed.

## 🚀 Quick Start

If you haven't already, download and install [Python](https://www.python.org/downloads/). There are several options to use the notebooks:

* From Visual Studio Code.
* From Jupyter Lab.

### Visual Studio Code

To use the notebooks directly from Visual Studio Code:

* Install the Python and Jupyter extensions from Microsoft in Visual Studio code.
* Use "Python: Create environment" and make sure to check the "requirements.txt" file to install also the dependencies.
* Open `01_parquet/00_from_json_to_sql.ipynb` to begin.
* Click "Run all" to run the notebook. You may be asked to select a runtime environment. Selct the ".venv" environment that you just created.

### Jupyter Lab

To use the notebooks through Jupyter Lab: Install the requirements and launch Jupter Lab, then open `notebooks/01_getting_started.ipynb` to begin.

```bash
pip install -r requirements.txt
jupyter lab
```

## Contributing analyses

Please remember to clear the output in your notebook before committing it.
