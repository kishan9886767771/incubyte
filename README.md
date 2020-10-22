# Solution for Incubyte Data Engineering Test
Solution for Incubyte Data Engineering Test is builded as a python package to read input files and store the records in MYSQL Database based on country.

## Assumptions / Considerations
- Datasets are available in Datasets Directory in csv and txt format.
- Running with python 3.x
- Following python packages are installed: \
-- PyYAML == 5.3.1 \
-- pandas == 1.1.3 \
-- SQLAlchemy == 1.3.20 \
-- mysqlclient == 2.0.1 

## Configuration File
- Update SQL Connection parameters and input directory path in config.yaml

## Executing the solution
To Execute the Spark Application with N CPUs and nGB driver memory
```
python run.py
```
