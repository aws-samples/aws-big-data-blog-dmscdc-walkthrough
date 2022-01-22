# Changelog
All notable changes to this project will be documented in this file.

## [1.1.0] - 2022-01-22
### Added
* Can use wild-card for schema and load multiple schema's at once
* Parallel processing of tables.
* Smarter repartition logic for tables with many partitions.
* Upgrade to Glue v3.0 (Faster start/run times)
* VPC options added to [DMSCDC_CloudTemplate_Reusable.yaml](DMSCDC_CloudTemplate_Reusable.yaml) to allow users to specify VPC settings and ensure replication instance can connect to the sources DB.
* [DMSCDC_SampleDB.yaml](DMSCDC_SampleDB.yaml) to easily test a source DB with a data load.

### Fixed
- Initial run of [DMSCDC_Controller.py](DMSCDC_Controller.py) was failing if DMS replication task was not loading
- New tables added after the initial deployment were not loading
- Incremental processing was not picking up new files correctly and was causing some files to be re-processed
- New columns added to tables were not being added to the data lake
