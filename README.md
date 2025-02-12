# Local Hudi Timeline Analysis

This tool provides an easy interface to access and analyze Hudi metadata without requiring customers to send their metadata to Onehouse lakeview. There are 3 parts to the tooling:

* **Hudi Timeline Extractor** - This tool grabs the Hudi timeline from the basepath (S3 folder) and creates a tarball which is stored locally in the file
* **Timeline Analyzer** - This tool lets users analyze the Hudi metadata from the downloaded tarballs and has 2 built in utilities
  * [Easy Lakeview Upload] - For users that are ok with Lakeview, but don't want the pain of configuration, this will allow easy access upload to Onehouse lakeview with a single command, dynamically creating all needed dependecies and configurations. However, this is just for quick testing - production deployments should use one of the deployment models specified in the Lakeview [docs](https://github.com/onehouseinc/LakeView).
  * [Local TCO Analyzer] - For users that don't want to (or are not able to) use Lakeview, this tool will analyze the .hoodie files and provide all of the inputs needed for the Onehouse cost calcualtor
* **Cleanup Utility** - Cleans up all danging .hoodie folders locally and ensures that unnecessary files are removed after we are able to create the TCO inputs

## Steps to Run

### Hudi Timeline Extractor

Ensure that you have access to the S3 buckets where the Hudi tables are stored. This can be through having Access Keys or by running this on an EC2 instance which has the getObject and listObject permissions for those directories. You can validate this by either running the provided requirements script or using the following aws cli command.

```bash
./scripts/initial_setup
```

OR

`aws s3 ls <your_s3_uri_path>`


You will execute the tar-hudi-timeline script for each table that you want to analyze the metadata for. You will execute this script using the following syntax for each table. Run it from the root directory of this project.

```bash
./scripts/tar-hudi-timeline <s3_basepath> <tableName>
```

This will create a directory in the /timelines/ folder for each table, where the hoodie metadata will live and be analyzed.


### Hudi Timeline Analyzer

Once you have all of the tables that you want to analyze, we can proceed to running the analyzer tool, this has 2 methods of operation

```bash
./scripts/update-all-timelines
```


#### Onehouse Lakeview Upload

If you are okay with using Onehouse lakeview, when prompted, enter y for the question "Do you want to use Lakeview"

This will automatically upload your hoodie metadata to Onehouse lakeview, where you will be able to see Onehouse's insights - your Onehouse Solutions Architect will be able to assist with viewing and interpreting these results for you


#### Local Analyzer

The analyzer can also be run locally. To use this, please select n for the question "Do you want to use Lakeview"

This will then automatically unpack all the tarball files and begin sampling the hoodie metadata to provide a report of the data volumes as well as the update and insert % of the tables.


#### Cleanup

Since we have locally processed a significant # of .hoodie folders, we may want to clean up these resources after execution. In order to do this, we will want to run the cleanup utility. This will find all unpacked .hoodie folders and remove them, while still keeping their source tarballs. 

```bash
./scripts/cleanup_hoodie_timelines
```
