# Radiology metadata

## Querying PACS

You can run `query.py` to execute batch queries of PACS with two subcommands (described in more detail below):

1. [`by_date`](#query-studies-by-date): Query studies over a range of dates
2. [`by_study`](#query-series-or-images-by-study): Query series or images from a table of study instance UIDs

The intent is first to collect broad, shallow study-level data for all the desired dates, and then collect more detailed
series- or image-level for valid studies from the first pass.

Each subcommand will iterate over a batch of queries and attempt to maximise efficiency by getting as many results as
possible for each query sent to PACS. Once the batch is complete, it will concatenate the extracted DICOM attributes to
a single dataframe in parquet format, and collect the logged statistics into a single CSV file.

### Initial configuration

You will need to run your queries from a machine with a connection to PACS already set up. Your local radiology IT team
can assist you in setting this up. They will provide you with your connection settings, which you must set in `query_config.toml`
under the `[pacs]` section. You will need:

- PACS IP address
- PACS port
- Called application entity title (AEC)
- [Optional] Calling application entity title (AET)

You can also modify the default behaviour of the script under the `[query]` section to control how many times queries will be
retried if they fail, how long to pause before retrying, and whether to overwrite previous results when querying studies by date.

You may want to reduce the retry limit if your queries are frequently rejected by PACS. If this happens, consult with radiology
IT to negotiate an appropriate level of resource use, e.g. only running a certain number of queries a day, or restricting queries
to off-peak hours. Retry behaviour is described in more detail below.

### Running the scripts in Docker

The [docker](../docker) directory lets you build a Docker image with all the dependencies necessary to query PACS. Use `build.sh`
to build the base image, then `add_user.sh` to add local user(s) to the image. You can then run an interactive docker container
or run a background container for a specific batch of queries.

#### Interactive

```bash
# Start interactive container
docker run -it --rm --name rad-ext-int --user $USER --network host --shm-size=64G \
  -v /path/to/code:/path/to/code -v /path/to/data:/path/to/data \
  user/rad-ext:version

# Once inside the container
cd /path/to/code/query_pacs
uv run query.py -h
```

Arguments:

- `-it` makes the container interactive and starts a shell
- `--rm` removes the container when you exit
- `--user $USER` make sure you run as user not root
- `--network host` ensures the image can access the host machine's network directly, so it can connect to PACS
- `--shm-size=64G` increases the virtual memory allocated to the container to prevent out of memory errors when handling large data tables
- `-v` these are examples of local volumes you may wish to mount in your container
- `user/rad-ext:version` replace this with the tag you used when building your image

The default entrypoint for the `rad-ext` image is `/bin/bash` so the interactive command will open to a bash shell.
You can then run `query.py` from there with `uv run`. Once the code is running, you may type CTRL-P, CTRL-Q to detach from the container
and leave it running. You can reattach with `docker attach rad-ext-int`.

#### Standalone batch job

```bash
docker run -itd --rm --name query_pacs --user $USER --network host --shm-size=64G \
  -v /path/to/code:/path/to/code -v /path/to/data:/path/to/data \
  user/rad-ext:version /path/to/code/query_studies_by_year.sh
```

Arguments:

- `-itd` detaches from the container after starting it
- `/path/to/code/query_studies_by_year.sh` is a bash wrapper script to run `query.py`

It is possible to set the entrypoint of the container to `query.py` and enter arguments to the script directly,
but using a bash wrapper simplifies the `docker` command. Some docker environments run into problems running
Python scripts directly rather than inside a bash wrapper (in my experience more likely inside a Run.AI / Kubernetes
environment). The current code was tested using Docker 27.3.1, build ce12230 under Ubuntu 22.04.05 LTS.

Examples:

- [query_studies_by_year.sh](query_studies_by_year.sh)
- [query_series_by_month.sh](query_series_by_month.sh)

### Query studies by date

```text
usage: query by_date [-h] --start_date START_DATE --end_date END_DATE --output_dir OUTPUT_DIR [--base_query BASE_QUERY]
                     [--overwrite]

options:
  -h, --help            show this help message and exit
  --start_date START_DATE
                        Start date in datetime-like format.
  --end_date END_DATE   End date in datetime-like format.
  --output_dir OUTPUT_DIR
                        Output directory for query results.
  --base_query BASE_QUERY
                        DICOM file defining query.
  --overwrite           If true, delete existing output, otherwise attempt to resume, skipping existing output.
```

The `by_date` subcommand will iterate over each day in the date range you provide, then over each hour in that day, and
query all studies for that hour. If the query fails, it will narrow the range to ten-minute intervals and try again.
If that fails, it will move on to the next hour, until every date in the range has been attempted.

The base query defines which fields shall be queried. It must contain the attribute `QueryRetrieveLevel` and this must
be set to `STUDY`. You can either provide a DICOM file defining your query or allow the script to use the default study
query defined in [query_definitions.py](query_definitions.py).

Each set of results are saved
as a Pandas DataFrame in parquet format along with the query statistics in JSON format, e.g.:

- `20250101_100000-105959.parquet`
- `20250101_100000-105959_stats.json`

After every date in the specified range has either been queried or failed, the DICOM dataframes and query statistics
will be concatenated into:

- `study_index.parquet`
- `study_query_stats.json`

The query stats file includes:

- `query_name` (e.g. 20250101_100000-105959)
- `status` (success / timeout / association rejected)
- `responses` (number of studies returned for this date and time range)
- `start_time`
- `end_time`
- `duration`
- `timeouts`
- `rejections`

If a query times out or is rejected by PACS, the script will pause and retry until a retry limit is reached. After
too many timeouts, it will move on to the next query. After too many rejections, it will abort the run.

Queries that fail after all retries will have stat files logged for them, but no data file, since no response was received.

Timeouts may occur temporarily because PACS is busy or the particular query gives too many results. They are likely
to resolve on their own, so the run will continue even if a particular query times out multiple times.

Rejections may be a sign that PACS is down or that our querying node has been blocked for making too many queries,
so may require human intervention to fix. That is why they cause the run to abort.

### Query series or images by study

```text
usage: query.py by_study [-h] --study_index STUDY_INDEX --output_dir OUTPUT_DIR [--query_level {series,image} | --base_query BASE_QUERY] [--chunk_size CHUNK_SIZE]
                         [--min_studies_per_chunk MIN_STUDIES_PER_CHUNK]

options:
  -h, --help            show this help message and exit
  --study_index STUDY_INDEX
                        Pandas DataFrame in parquet format containing study metadata.
  --output_dir OUTPUT_DIR
                        Directory to save series query DICOM files.
  --query_level {series,image}
                        Query level (series or image). Required if --base_query not provided.
  --base_query BASE_QUERY
                        DICOM file defining base query. Required if --query_level not provided.
  --chunk_size CHUNK_SIZE
                        Number of series or images to query at a time. [300]
  --min_studies_per_chunk MIN_STUDIES_PER_CHUNK
                        Minimum number of studies per chunk (below which query studies individually). [10]
```

The `by_study` subcommand iterates over the study index - a table of Study Instance UIDs. This may be created by
running the `by_date` subcommand and then trimming the output to include only studies meeting your requirements.
It will divide the study index into chunks and iteratively query each chunk. After one pass through the study index,
if any queries failed, it will halve the chunk size, divide the unqueried studies into smaller chunks, and try
another pass. If the number of studies per chunk falls below `min_studies_per_chunk`, then it will query each study
individually then finish.

Just like querying by date, each queried chunk or study will produce a DataFrame in parquet format (unless it fails)
and a stat file in JSON format. At the end of the run, these are concatenated.

The study index must contain the DICOM attributes `StudyInstanceUID`, `NumberOfStudyRelatedSeries`, and if querying
images, `NumberOfStudyRelatedInstances`.

The base query defines which fields shall be queried. It must contain the attribute `QueryRetrieveLevel` and this must
be set to `SERIES` or `IMAGE`. You can either provide a DICOM file defining your query or allow the script to use the 
default series or image queries defined in [query_definitions.py](query_definitions.py).

Outputs:

- Individual results from chunked queries
  - `pass001_chunk0002.parquet`
  - `pass001_chunk0002_stats.json`
- Individual results from single study queries (suffixed by UID)
  - `pass004_study_1.2.3.4.5.parquet`
  - `pass004_study_1.2.3.4.5_stats.json`
- Concatenated results
  - `series_index.parquet`
  - `series_query_stats.csv`
  - OR
  - `image_index.parquet`
  - `image_query_stats.csv`
- `study_index_queried.parquet` the original study index with pass, chunk, and status filled in for each study

The stats file includes:

- `status`
- `responses`
- `start_time`
- `end_time`
- `duration`
- `timeouts`
- `rejections`
- `pass`
- `chunk` (or "study" if querying single studies)
- `first_study_id`
- `last_study_id` (same as first if querying single studies)
- `num_queried_studies`
- `num_queried_series`
- `num_queried_images` (only if querying images)
- `num_found_studies`
- `num_found_series`
- `num_found_images` (only if querying images)

Timeouts and rejections are handled in the same way as querying by date, except the retry limit for chunked queries
is smaller, and if it is reached for rejected associations the script will go directly to querying single studies
instead of aborting, and then abort if the retry limit for rejected associations is reached for single studies.

### Logging

The output from each run will be saved to a log file prefixed `run_` and suffixed by the run start datetime. The script includes loggers to stdout, to file, or to both, so you can customise the output.

Realtime output will include progress bars from `tqdm` which are written to stdout and therefore not included in the log file.

### Works in progress

Old scripts for pulling images are preserved in the [wip](wip) subdirectory. These used an old approach where
PACS settings were stored as environment variables, but otherwise use similar calls to pydicom and pynetdicom.
To pull images, you need two active processes: one to request the images and another to receive them.

In these scripts, the receiver is configured to save only the first image from each series to allow
retrieval of attributes not indexed for queries. This may be superceded by using [DICOMWEB WADO](https://www.dicomstandard.org/using/dicomweb/retrieve-wado-rs-and-wado-uri) queries.
