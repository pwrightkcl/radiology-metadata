import tomllib
import argparse
from datetime import date, datetime
from time import sleep, time, localtime, strftime
from pathlib import Path
import math
import warnings
import json
import logging
import sys
from dataclasses import dataclass, field
from typing import Union, Sequence, Any
import copy

from pydantic_settings import BaseSettings, SettingsConfigDict
from tqdm import tqdm
import pandas as pd
from pandas.api.types import is_list_like
from pydicom import dcmread, Dataset, DataElement
from pydicom.datadict import dictionary_VM, dictionary_has_tag
from pydicom.valuerep import DSfloat, IS, PersonName
from pydicom.uid import UID
from pynetdicom import AE
from pynetdicom.status import code_to_category
from pynetdicom.sop_class import StudyRootQueryRetrieveInformationModelFind  # type: ignore[attr-defined, import-not-found]

from query_definitions import make_study_query, make_series_query, make_image_query
from parse_query_stats import stats_json_to_dataframe


_LOG_SETUP = False

# Reference DICOM VR sets from https://dicom.nema.org/medical/dicom/current/output/chtml/part05/sect_6.2.html#table_6.2-1
STRING_VRS = {"AE", "AS", "CS", "DA", "DT", "LO", "LT", "PN", "SH", "ST", "TM", "UC", "UI", "UR", "UT"}
NUMERIC_VRS = {"DS", "FL", "FD", "IS", "SL", "SS", "UL", "US"}


class FileOnlyFilter(logging.Filter):
    """Logging filter to allow only records with `to_file` attribute set to True."""
    def filter(self, record):
        """Filter the record to allow only those with `to_file` attribute set to True.
        
        Args:
            record: Log record to filter."""
        return getattr(record, 'to_file', False)


class StdoutOnlyFilter(logging.Filter):
    """Logging filter to allow only records with `to_stdout` attribute set to True."""
    def filter(self, record):
        """Filter the record to allow only those with `to_stdout` attribute set to True.
        
        Args:
            record: Log record to filter."""
        return getattr(record, 'to_stdout', False)


class DestLogger(logging.LoggerAdapter):
    """Logger adapter to filter logs to file, stdout, or both based on the `dest` dictionary passed at initialisation.
    """
    def __init__(self, logger, dest):
        """Initialise the DestLogger with a logger and a destination dictionary.

        Args:
            logger: Logger to adapt.
            dest (dict): Dictionary with keys 'to_file' and 'to_stdout' indicating where to log messages.
        """
        super().__init__(logger, {})
        self.dest = dest
    def process(self, msg, kwargs):
        """Process the log message and add the destination information to the kwargs.
        
        Args:
            msg: Log message to process.
            kwargs: Keyword arguments for the log message."""
        kwargs['extra'] = self.dest
        return msg, kwargs


class PACSSettings(BaseSettings):
    """Settings for PACS connection.
    
    Attributes:
        ip (str): IP address of the PACS server
        port (int): Port number of the PACS server (note that ports below 1024 may require root privileges)
        aec (str): Called Application Entity Title (PACS server)
        aet (str): Calling Application Entity Title (querying node)
    """
    ip: str
    port: int
    aec: str
    aet: str

    model_config = SettingsConfigDict(extra='forbid')


class QuerySettings(BaseSettings):
    """Settings for querying PACS, with defaults for retry limits and overwriting.
    
    Attributes:
        retry_limit (int): Number of times to retry after PACS association rejected when querying studies by date
        chunk_retry_limit (int): Retries when querying series or images by study UID, for chunks of study UIDs
        study_retry_limit (int): Retries when querying series or images by study UID, for single study UIDs
        retry_pause_seconds (int): Seconds to pause between retry attempts after a timeout or association rejection
        overwrite (bool): Overwrite existing data; only applies when querying studies by date
    """
    retry_limit: int = 10
    chunk_retry_limit: int = 3
    study_retry_limit: int = 10
    retry_pause_seconds: int = 120
    overwrite: bool = False


@dataclass
class QueryRunDate():
    """Holds attributes that input to or modified by each query by date and time range.
    
    It is initialised in `__main__` with settings. For each query, it is updated with the date and time range,
    then passed to `query_datetimes`, which updates the status and adds to the list of stats and data files.
     
    Attributes:
        pacs_settings (PACSSettings): PACS connection settings.
        query_settings (QuerySettings): Query behaviour settings.
        ae (AE): pynetdicom Application Entity used for associations.
        output_dir (Path): Directory to save query results and stats.
        base_query (Dataset): pydicom Dataset defining the base query to modify with date and time for each query.
        start_date (str): Start date for query range in datetime-like format.
        end_date (str): End date for query range in datetime-like format.
        query_date (str): Date to query in YYYYMMDD format.
        query_time_range (str): Time range to query in HHMMSS-HHMMSS format.
        status (str): Status of the last query (e.g., 'Success', 'Failed', 'Association Rejected').
        stat_files (list[Path]): List of paths to stat files created during the run.
        data_files (list[Path]): List of paths to data files created for each query in this run.
    """
    # Startup attributes:
    pacs_settings: PACSSettings
    query_settings: QuerySettings
    ae: AE
    output_dir: Path
    base_query: Dataset
    start_date: str
    end_date: str

    # Changed for each query:
    query_date: str = ''
    query_time_range: str = ''
    status: str = ''

    # Filled during run:
    stat_files: list[Path] = field(default_factory=list)
    data_files: list[Path] = field(default_factory=list)

    def set_query_date(self, value: Union[str, date, datetime]):
        """Convert string or data/datetime to DICOM date string (YYYYMMDD).
        
        Args:
            value (Union[str, date, datetime]): Date value to convert.
        
        Returns:
            self: Updated QueryRunDate object with the `query_date` attribute set to the converted date string.
        """
        if isinstance(value, (date, datetime)):
            self.query_date = value.strftime("%Y%m%d")
        else:
            self.query_date = str(value).strip()

        return self

    def setup_query(self, date_value: Union[str, date, datetime], time_range: str):
        """Set attributes for a single query by date and time range.

        Set per-query fields. Do minimal normalisation only.
        Use `apply_to(ds)` to assign to a pydicom Dataset so pydicom can validate formats/VRs.

        Args:
            date_value (Union[str, date, datetime]): Date to query (will be converted to DICOM date string).
            time_range (str): Time range to query in HHMMSS-HHMMSS
        
        Returns:
            self: Updated QueryRunDate object with the following attributes modified:
                query_date: Updated with the provided date value, converted to DICOM date string format.
                query_time_range: Updated with the provided time range string.
        """
        self.set_query_date(date_value)
        self.query_time_range = time_range

        return self

    def add_stat_file(self, p: Union[Path, str]):
        """Append the filename of the stat file to the list, converting to Path if necessary.
        
        Args:
            p (Union[Path, str]): Path or string of the stat file created for this query.
            
        Returns:
            self: Updated QueryRunDate object with the stat file added to the `stat_files` list.
        """
        p = Path(p)
        self.stat_files.append(p)
        return self

    def add_data_file(self, p: Union[Path, str]):
        """Append the filename of the data file to the list, converting to Path if necessary.
        
        Args:
            p (Union[Path, str]): Path or string of the data file created for this query.
        
        Returns:
            self: Updated QueryRunDate object with the data file added to the `data_files` list.
        """
        p = Path(p)
        self.data_files.append(p)
        return self


@dataclass
class QueryRunStudy():
    """Holds attributes that input to or modified by each pass of queries by study instance UID.

    It is initialised in `__main__` with settings. For each pass, it is updated with the dataframe and other inputs,
     then passed to `query_dataframe`, which updates the status and adds to the list of stats and data files.

    Attributes:
        pacs_settings (PACSSettings): PACS connection settings.
        query_settings (QuerySettings): Query behaviour settings.
        ae (AE): pynetdicom Application Entity used for associations.
        output_dir (Path): Directory to save query results and stats.
        base_query (Dataset): pydicom Dataset defining the base query to modify with StudyInstanceUID for each query.
        study_index (Path): Path to the parquet file containing the study index.
        chunk_size (int): Initial chunk size for querying studies in batches.
        min_studies_per_chunk (int): Minimum number of studies per chunk before switching to single-study queries.
        pass_si: Study index containing chunks or single studies for this pass.
        pass_num (int | None): Current pass number.
        chunked (bool | None): Is this pass querying chunks of studies (True) or single studies (False)?
        status (str): Status of the last query (e.g., 'Success', 'Failed', 'Association Rejected').
        stat_files (list[Path]): List of paths to stat files created during the run.
        data_files (list[Path]): List of paths to data files created for each query in this pass.
    """

    # Startup attributes:
    pacs_settings: PACSSettings
    query_settings: QuerySettings
    ae: AE
    output_dir: Path
    base_query: Dataset
    study_index: Path
    chunk_size: int
    min_studies_per_chunk: int

    # Changed for each pass:
    pass_si: pd.DataFrame = field(default_factory=pd.DataFrame)
    pass_num: int | None = None
    chunked: bool | None = None

    # Changed for each query:
    status: str = ''

    # Filled during run:
    stat_files: list[Path] = field(default_factory=list)
    data_files: list[Path] = field(default_factory=list)

    def setup_pass(self, pass_si: pd.DataFrame, pass_num: int, chunked: bool):
        """Set attributes for a single pass of queries by dataframe.
        
        Args:
            pass_si (pd.DataFrame): DataFrame containing studies (or chunks of studies) to query. Must include column 'StudyInstanceUID'.
            pass_num (int): Current pass number.
            chunked (bool): Is this pass querying chunks of studies (True) or single studies (False)?

        Returns:
            self: Updated QueryRunStudy object with the following attributes modified:
                pass_si: Updated with the provided DataFrame.
                pass_num: Updated with the provided pass number.
                chunked: Updated with the provided chunked value.
        """
        self.pass_si = pass_si
        self.pass_num = pass_num
        self.chunked = chunked

        return self

    def add_stat_file(self, p: Union[Path, str]):
        """Append the filename of the stat file to the list, converting to Path if necessary.
        
        Args:
            p (Union[Path, str]): Path or string of the stat file created for this query.
            
        Returns:
            self: Updated QueryRunStudy object with the stat file added to the `stat_files` list.
        """
        p = Path(p)
        self.stat_files.append(p)
        return self

    def add_data_file(self, p: Union[Path, str]):
        """Append the filename of the data file to the list, converting to Path if necessary.
        
        Args:
            p (Union[Path, str]): Path or string of the data file created for this query.
            
        Returns:
            self: Updated QueryRunStudy object with the data file added to the `data_files` list.
        """
        p = Path(p)
        self.data_files.append(p)
        return self


def setup_logger(log_file: Path | str | None = None, log_level: int = logging.INFO) -> logging.Logger:
    """Set up a logger with file and stdout handlers.
    
    Args:
        log_file (Path | str | None): Optional path to a log file. If None, logs only to stdout.
        log_level (int): Logging level (default=logging.INFO).
    
    Returns:
        logging.Logger: Configured logger instance.
    """
    global _LOG_SETUP
    logger = logging.getLogger("query_pacs")
    logger.setLevel(log_level)
    logger.handlers.clear()

    formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')

    # File handler
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        file_handler.addFilter(FileOnlyFilter())
        logger.addHandler(file_handler)

    # Stdout handler
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    stream_handler.addFilter(StdoutOnlyFilter())
    logger.addHandler(stream_handler)

    _LOG_SETUP = True

    return logger


def get_loggers() -> tuple[DestLogger, DestLogger, DestLogger]:
    """Get configured loggers that filter output to file, stdout, or both.
    
    Requires `setup_logger` to have been called first.
    
    Returns:
        tuple[DestLogger, DestLogger, DestLogger]: Three loggers:
            log2file: Logger that logs only to file.
            log2stdout: Logger that logs only to stdout.
            log2both: Logger that logs to both file and stdout.
    """
    if not _LOG_SETUP:
        raise RuntimeError("Called `get_loggers` before `setup_logger`.")
    logger = logging.getLogger("query_pacs")
    log2file = DestLogger(logger, {'to_file': True, 'to_stdout': False})
    log2stdout = DestLogger(logger, {'to_file': False, 'to_stdout': True})
    log2both = DestLogger(logger, {'to_file': True, 'to_stdout': True})

    return log2file, log2stdout, log2both


def log_raise(error: type[Exception], msg: str) -> None:
    """Log an error message and then raise the exception.
    
    Args:
        error (type[Exception]): Exception class to raise.
        msg (str): Error message to log and include in the raised exception.
    
    Raises:
        Exception: The specified error class with the provided message.
    """
    log2file, log2stdout, log2both = get_loggers()
    log2both.error(msg)
    raise error(msg)


def my_time(time_in_seconds: float) -> str:
    """Convert seconds from epoch to string for logging.
    
    Args:
        time_in_seconds (float): Time in seconds from epoch.
        
    Returns:
        str: Formatted time string in 'YYYY-MM-DD HH:MM:SS' format.
    """
    return strftime('%Y-%m-%d %H:%M:%S', localtime(time_in_seconds))


def concatenate_saved_dataframes(data_files: Sequence[Path|str]) -> pd.DataFrame:
    """Concatenate saved parquet or pickle data files into a single dataframe.

    Args:
        data_files (Sequence[Path|str]): List of parquet or pickle files as Paths or strings.

    Returns:
        pd.DataFrame: Concatenated dataframe.
    """
    log2file, log2stdout, log2both = get_loggers()
    dataframes = []
    for f in tqdm(data_files, desc="Loading data files", unit="file", total=len(data_files)):
        f = Path(f) if isinstance(f, str) else f
        if f.suffix == '.parquet':
            try:
                df = pd.read_parquet(f)
                df['filename'] = f.name
                dataframes.append(df)
                continue
            except Exception as e:
                log2both.warning(f"Error loading {f} as parquet: {e}")
        elif f.suffix == '.pkl':
            try:
                df = pd.read_pickle(f)
                df['filename'] = f.name
                dataframes.append(df)
                continue
            except Exception as e:
                log2both.warning(f"Error loading {f} as pickle: {e}")
        else:
            log2both.warning(f"Incorrect file suffix, must be .parquet or .pkl: {f}")

    if dataframes:
        concatenated_df = pd.concat(dataframes, ignore_index=True)
        log2both.info(f"Concatenated {len(dataframes)} data files into dataframe with {len(concatenated_df)} rows.")
    else:
        concatenated_df = pd.DataFrame()
        log2both.warning("No data files loaded successfully. Returning empty dataframe.")

    return concatenated_df


def dataset_to_dict(ds: Dataset, prefix: str = "") -> dict:
    """Convert all the elements in a pydicom Dataset to a dictionary, flattening sequences.

    Sequence elements will be named 'SequenceKeyword.<i>.<NestedElementKeyword>', where I is the index
    of the element in the sequence.

    Ignore PixelData element (7FE0,0010).

    Args:
        ds (Dataset): pydicom Dataset to convert.
        prefix (str): Prefix to key, used for nested elements.

    Returns:
        dict: Dictionary of DICOM tags.
    """
    dicom_dict = {}

    for key in ds.keys():
        if key == (0x7FE0,0x0010):
            continue  # skip PixelData

        el = ds[key]

        if not el.is_private and el.keyword:
            _key = prefix + el.keyword
        else:
            # Make a string from the hex tag that is human-readable, usable as a Pandas column,
            # and still works as a key for indexing the Dataset object.
            _key = prefix + f'0x{el.tag:08x}'

        if el.VR == 'SQ':
            # Handle sequences
            for i, sq_ds in enumerate(el.value):
                sq_prefix = prefix + f'{_key}.{i}.'
                sq_dict = dataset_to_dict(sq_ds, sq_prefix)
                dicom_dict.update(sq_dict)
            continue

        # Handle regular elements
        norm = _normalise_vr(el)
        dicom_dict[_key] = _convert_value(norm)

    return dicom_dict


def _normalise_vr(el: DataElement) -> Any:
    """Normalise a DICOM element's value according to its value representation (VR) and value multiplicity (VM).

    Prevent pyarrow errors when saving the dataframe to parquet. Pyarrow does not
    recognise pydicom MultiValue type, so these are converted to lists. Pyarrow can reject columns with
    mixed types, e.g. both `list` and `float` if missing values are stored as NaN. This function should
    ensure each column is typed consistently based on the DICOM dictionary.
    
    The function was first introduced to catch ImageType values that are empty strings or improperly
    formatted as singleton strings like "DERIVED/SECONDARY" or "DERIVED" rather than MultiValue of strings.
    
    Rules:
        - MultiValue elements always converted to list
        - VM = 1 & VR is numeric: "" -> NaN
        - VM = 1 & VR is string: NaN -> ""
        - VM > 1: convert to list, splitting strings on slash or backslash delimiters
        - VM = 1 & value is a list:
            - [] -> NaN or "" depending on VR
            - [""] or [NaN] -> NaN or "" depending on VR
            - Single non-missing values returned as singletons
            - Multivalue lists converted to string

    Args:
        el (DataElement): pydicom DataElement

    Returns:
        Union[Any, list]: Normalised value according to VR and VM
    """
    value = el.value
    vr = el.VR
    tag = el.tag

    my_null = "" if vr in STRING_VRS else None

    # Get VM from dictionary if known
    if dictionary_has_tag(tag):
        vm = dictionary_VM(tag)  # e.g., "1", "1-n", "2", "2-n"
    else:
        return value

    if is_list_like(value) and not isinstance(value, (str, bytes, bytearray)):
        if vm == "1":
            # VM = 1 and value is list-like (rare): flatten or coerce
            if len(value) == 0:
                return my_null
            if len(value) == 1:
                if value[0] == "" or value[0] is None or pd.isna(value[0]):
                    return my_null
                return value[0]
            return str(value)
        return list(value)

    # Scalar value path
    if vm != "1":
        # VM > 1 so convert scalar to list
        if value == "" or value is None or pd.isna(value):
            return []
        if isinstance(value, str):
            if '/' in value:
                return value.split('/')
            if '\\' in value:
                return value.split('\\')
            return [value]
        return [value]
    if value == "" or value is None or pd.isna(value):
        return my_null

    # Value is already normal. Return unchanged.
    return value


def _convert_value(v: Any) -> Any:
    """Convert pydicom value to a serializable value.

    Source: https://github.com/pydicom/pydicom/discussions/1673#discussioncomment-3417403

    Args:
        v (Any): pydicom value to convert.

    Returns:
        Converted value.
    """
    if is_list_like(v) and not isinstance(v, (str, bytes, bytearray)):
        return [_convert_value(mv) for mv in v]
    if pd.isna(v):
        return None
    if type(v) is DSfloat:
        return float(v) if str(v) != "" else None
    if type(v) is IS:
        return int(v) if str(v) != "" else None
    if type(v) in (int, float):
        return v
    if type(v) in (str, PersonName, UID):
        cv = _sanitise_unicode(str(v))
        if len(cv) > 256:
            cv = cv[:253] + '...'
        return cv
    if type(v) is bytes:
        return _sanitise_unicode(v.decode('ascii', 'replace'))
    if type(v) is bytearray:
        return _sanitise_unicode(v.decode('ascii', 'replace'))
    return repr(v)


def _sanitise_unicode(s: str) -> str:
    """Remove null characters and strip whitespace from a string.
    
    Args:
        s (str): String to sanitise.
    
    Returns:
        str: String with null characters removed and whitespace stripped.
    """
    return s.replace(u"\u0000", "").strip()


def query_datetimes(q: QueryRunDate) -> QueryRunDate:
    """Run a query for a specific date and time range.

    Save query results and logged stats for this date and time range to output_dir.
    Modify `q` by updating `status` and adding saved filenames to `stat_files` and `data_files`.

    Args:
        q (QueryRunDate): contains settings and date/time range for the query.

    Returns:
        QueryRunDate object with the following attributes modified:
            status: Status of the last query (e.g., 'Success', 'Failed', 'Association Rejected').
            stat_files: Append any stat file created for this query.
            data_files: Append any data file created for this query.
    """
    log2file, log2stdout, log2both = get_loggers()
    query_name = '_'.join([q.query_date, q.query_time_range])
    data_file = q.output_dir / f'{query_name}.parquet'
    stat_file = q.output_dir / f'{query_name}_stats.json'

    if not q.query_settings.overwrite and (data_file.exists() or stat_file.exists()):
        q.status = 'skipped'
        return q

    # Set up the query dataset
    query_ds = copy.deepcopy(q.base_query)
    # Future: make it so it can handle Series and Instance times for other levels (not used yet and may never be)
    query_ds.StudyDate = q.query_date
    query_ds.StudyTime = q.query_time_range

    # Run the query
    ds_list, query_stats = run_query(q, query_ds)

    query_stats['query_name'] = query_name
    with stat_file.open('w') as f:
        json.dump(query_stats, f, indent=2)
    q.add_stat_file(stat_file)

    q.status = query_stats['status']
    if q.status.lower() == 'success':
        if len(ds_list) > 0:
            response_df = pd.DataFrame([dataset_to_dict(ds) if ds else None for ds in ds_list])
            try:
                response_df.to_parquet(data_file)
                q.add_data_file(data_file)
            except Exception as e:
                log2both.warning(f"Error saving {data_file}: {e}")
                data_file_pickle = data_file.with_suffix('.pkl')
                log2both.info(f"Saving as pickle to {data_file_pickle}.")
                response_df.to_pickle(data_file_pickle)
                q.add_data_file(data_file_pickle)

    return q


def query_dataframe(q: QueryRunStudy) -> QueryRunStudy:
    """Query studies or chunks of studies from a dataframe and save results.

    Process a single pass set up by `query_by_study`, querying each row and updating the dataframe and file lists.

    Args:
        q (QueryRunStudy): contains settings and index of studies / chunks of studies for this pass.

    Returns:
        QueryRunStudy: Updated object with status, stat_files, and data_files modified.
    """
    q.status='pending'
    retry_limit = q.query_settings.chunk_retry_limit if q.chunked else q.query_settings.study_retry_limit

    for i, row in tqdm(q.pass_si.iterrows(), total=len(q.pass_si)):
        if q.chunked:
            suffix = f"chunk{row['chunk']:04.0f}"
        else:
            suffix = f"study_{row['StudyInstanceUID']}"

        data_file = q.output_dir / f'pass{q.pass_num:03.0f}_{suffix}.parquet'
        stat_file = q.output_dir / f'pass{q.pass_num:03.0f}_{suffix}_stats.json'

        # Check for existing data
        if data_file.exists():
            log_raise(FileExistsError, f"Response data file already exists: {data_file}")
        if stat_file.exists():
            log_raise(FileExistsError, f"Response metadata file already exists: {stat_file}")

        # Get default query Dataset
        this_query = q.base_query.copy()
        this_query_level = q.base_query['QueryRetrieveLevel'].value.lower()

        # Set StudyInstanceUIDs from current chunk
        # Mute warnings because UID strings are sometimes invalid
        with warnings.catch_warnings():
            this_query.StudyInstanceUID = row['StudyInstanceUID']

        # Query the study
        ds_list, query_stats = run_query(q, this_query, retry_limit=retry_limit)

        # Complete the query stats
        query_stats['pass'] = q.pass_num
        query_stats['chunk'] = row['chunk'] if q.chunked else 'study'
        sid_list = row['StudyInstanceUID'].split('\\')  # May be just one
        query_stats.update({'first_study_id': sid_list[0], 'last_study_id': sid_list[-1],
                'num_queried_studies': len(sid_list), 'num_queried_series': row['NumberOfStudyRelatedSeries'],})
        if this_query_level == 'image':
            query_stats['num_queried_instances'] = row['NumberOfStudyRelatedInstances']

        q.status = query_stats['status']
        q.pass_si.loc[i, 'status'] = query_stats['status']  # type: ignore[call-overload]

        if q.status.lower() == 'success':
            if ds_list:
                response_df = pd.DataFrame([dataset_to_dict(ds) if ds else None for ds in ds_list])
                if this_query_level == 'image':
                    query_stats['num_found_instances'] = len(response_df)
                    # Keep only the first of each SeriesInstanceUID
                    response_df = response_df.sort_values('SeriesInstanceUID').drop_duplicates('SeriesInstanceUID')
                query_stats['num_found_series'] = len(response_df)
                query_stats['num_found_studies'] = len(response_df['StudyInstanceUID'].unique())
                try:
                    response_df.to_parquet(data_file)
                    q.add_data_file(data_file)
                except Exception as e:
                    log2both.warning(f"Error saving {data_file}: {e}")
                    data_file_pickle = data_file.with_suffix('.pkl')
                    log2both.info(f"Saving as pickle to {data_file_pickle}.")
                    response_df.to_pickle(data_file_pickle)
                    q.add_data_file(data_file_pickle)
            else:
                if this_query_level == 'image':
                    query_stats['num_found_instances'] = 0
                query_stats['num_found_series'] = 0
                query_stats['num_found_studies'] = 0
            q.pass_si.loc[i, 'queried'] = True  # type: ignore[call-overload]
        else:
            if this_query_level == 'image':
                query_stats['num_found_instances'] = 0
            query_stats['num_found_series'] = 0
            query_stats['num_found_studies'] = 0

        with stat_file.open('w') as f:
            json.dump(query_stats, f, indent=2)
        q.add_stat_file(stat_file)

        if q.status.lower() == 'association rejected':
            # Abort the pass because the association was rejected more times than the retry limit.
            return q

    return q


def run_query(
    q: QueryRunDate | QueryRunStudy,
    ds: Dataset,
    retry_limit: int | None = None,
) -> tuple[list[Dataset | None], dict]:
    """Run a `send_c_find` query command and return response and status.

    Args:
        q (QueryRunDate | QueryRunStudy): Query run instance with PACS settings, application entity, and query configuration.
        ds (pydicom.Dataset): pydicom Dataset defining the query.
        retry_limit (int | None): Number of times to retry query if rejected. If None, uses q.query_settings.retry_limit.

    Returns:
        ds_list (list): list of pydicom Datasets, one for each query response.
        query_stats (dict): Query statistics:
            status (str): Status extracted from the response.
            responses (int): Number of responses received.
            start_time (str): Start time of the query.
            end_time (str): End time of the query.
            duration (float): Duration of the query in seconds.
            timeouts (int): Number of timeouts encountered.
            rejections (int): Number of association rejections encountered.
    """
    tic = time()
    log2file, log2stdout, log2both = get_loggers()
    
    if retry_limit is None:
        retry_limit = q.query_settings.retry_limit
    retry_pause_seconds = q.query_settings.retry_pause_seconds
    
    status = 'pending'
    ds_list = []
    tries = 0
    timeout = 0
    rejected = 0
    while status == 'pending':
        tries += 1
        assoc = q.ae.associate(
            q.pacs_settings.ip,
            q.pacs_settings.port,
            ae_title=q.pacs_settings.aec,
        )
        if assoc.is_established:
            responses = assoc.send_c_find(ds, StudyRootQueryRetrieveInformationModelFind)
            for (status_ds, identifier) in responses:
                if status_ds:
                    if status_ds.Status in (0xFF00, 0xFF01):  # Pending responses
                        ds_list.append(identifier)
                    else:
                        status = code_to_category(status_ds.Status)
                elif tries < retry_limit:
                    timeout += 1
                    status = 'pending'  # should already be pending
                    sleep(retry_pause_seconds)
                else:
                    timeout += 1
                    status = 'peer timed out, aborted or sent an invalid response'
            assoc.release()
        else:
            log2both.warning(f"PACS association attempt {tries+1} rejected.")
            print("Association rejected")
            if tries < retry_limit:
                rejected += 1
                status = 'pending'  # should already be pending
                sleep(retry_pause_seconds)
            else:
                rejected += 1
                status = 'association rejected'
    toc = time()
    query_stats = {
        "status": status,
        "responses": len(ds_list),
        "start_time": my_time(tic),
        "end_time": my_time(toc),
        "duration": round(toc - tic, 1),
        "timeouts": timeout,
        "rejections": rejected,
    }

    return ds_list, query_stats


def query_studies_by_date(q: QueryRunDate) -> None:
    """Query studies by date range.

    Set up a loop of queries through the given date range, querying each hour, and narrowing to 10-minute intervals if a query fails.
    After completing the date range, concatenate the resulting DICOM tables and query stat log files.

    Args:
        q (QueryRunDate): contains settings and date range for the run.

    Returns:
        None. Writes query responses and logged statistics to output directory.
    """
    run_start = time()
    log2file, log2stdout, log2both = get_loggers()
    log2both.info("Querying studies by date range.")
    log2both.info(f"Date range: {q.start_date} to {q.end_date}.")

    q.output_dir = q.output_dir.resolve()
    log2stdout.info(f"Using output directory: {q.output_dir}")

    abort_run = False
    date_range = pd.date_range(q.start_date, q.end_date)
    for this_date in tqdm(date_range, desc="Querying dates", unit="date", total=len(date_range)):
        if abort_run:
            break

        # Query by hour
        hour_range = range(24)
        for hour in tqdm(hour_range, desc="Querying hours", unit="hour", total=len(hour_range)):
            if abort_run:
                break

            q.setup_query(this_date, f"{hour:02}0000-{hour:02}5959")
            query_datetimes(q)

            if q.status.lower() == 'association rejected':
                abort_run = True
                break
            elif q.status.lower() != 'success':
                # Status is failed, timed out, or skipped.
                # Maybe one hour returned too many responses, so try a smaller time range
                # No need for `if abort_run` because we cannot reach here if it is true
                minute_range = range(0, 60, 10)
                for minute in tqdm(minute_range, desc="Querying minutes", unit="minute", total=len(minute_range)):

                    q.setup_query(this_date, f"{hour:02}{minute:02}00-{hour:02}{minute + 9:02}59")
                    query_datetimes(q)

                    if q.status.lower() == 'association rejected':
                        abort_run = True
                        break
    if abort_run:
        log2both.warning(f"Aborting run after association rejected {q.query_settings.retry_limit} times.\n")
    run_end = time()
    log2both.info("END OF RUN")
    log2both.info(f"Run duration: {strftime('%H:%M:%S', localtime(run_end - run_start))}")

    if q.stat_files:
        log2both.info("Parsing query stats files.")
        stats_json_to_dataframe(q.stat_files).to_csv(q.output_dir / 'study_query_stats.csv', index=False)
    if q.data_files:
        log2both.info("Concatenating DICOM query responses to study index.")
        study_index = concatenate_saved_dataframes(q.data_files)
        si_file = q.output_dir / 'study_index.parquet'
        log2both.info(f"Saving study index to {si_file}.")
        try:
            study_index.to_parquet(si_file)
        except Exception as e:
            log2both.warning(f"Error saving {si_file}: {e}")
            si_file_pickle = si_file.with_suffix('.pkl')
            log2both.info(f"Saving as pickle to {si_file_pickle}.")
            study_index.to_pickle(si_file_pickle)


def query_by_study(q: QueryRunStudy):
    """Query series or images for each study in an index, chunking studies where possible.

    Divide the study index into chunks and attempt to iteratively query each chunk. If any queries fail, halve
    the chunk size and run another pass through the remaining studies. If the number of studies in  each chunk 
    falls below `min_studies_per_chunk`, query each study individually then end.

    The study index must contain the DICOM attributes StudyInstanceUID, NumberOfStudyRelatedSeries, and if querying
    images, NumberOfStudyRelatedInstances.
    
    The base query must contain the attribute 'QueryRetrieveLevel', which is used to determine the target.

    This script has not been made resumable because the responses depend on the PACS and may vary between runs.
    To resume an interrupted run, remake the study index removing the rows that were successfully queried in the 
    previous run.

    Args:
        q (QueryRunStudy): contains settings and study query run parameters.

    Returns:
        None. Writes query responses and logged statistics to output directory.
    """
    run_start = time()
    log2file, log2stdout, log2both = get_loggers()

    # Work out target from base_query
    if hasattr(q.base_query, 'QueryRetrieveLevel'):
        target = q.base_query['QueryRetrieveLevel'].value.lower()
        if target == 'series':
            targets = 'series'
            target_counter = 'NumberOfStudyRelatedSeries'
        elif target == 'image':
            targets = 'images'
            target_counter = 'NumberOfStudyRelatedInstances'
        else:
            raise ValueError("base_query QueryRetrieveLevel must be 'SERIES' or 'IMAGE'.")
    else:
        raise ValueError("base_query must have attribute 'QueryRetrieveLevel'.")

    log2both.info(f"Querying {targets} by StudyInstanceUID.")

    q.output_dir = Path(q.output_dir)
    log2both.info(f"Using output directory: {q.output_dir}")

    if not q.study_index.exists():
        log_raise(FileNotFoundError, f"Study index not found: {q.study_index}")

    load_si_start = time()
    si = pd.read_parquet(q.study_index)
    load_si_end = time()

    log2both.info(f"Loaded study index in {load_si_end - load_si_start:.2f} s. Found {len(si)} rows.")

    if 'StudyInstanceUID' not in si.columns:
        log_raise(ValueError, "Study index does not contain 'StudyInstanceUID' column.")
    if 'NumberOfStudyRelatedSeries' not in si.columns:
        log_raise(ValueError, "Study index does not contain 'NumberOfStudyRelatedSeries' column.")
    if target == 'image':
        if 'NumberOfStudyRelatedInstances' not in si.columns:
            log_raise(ValueError, "Study index does not contain 'NumberOfStudyRelatedInstances' column.")

    # Ignore studies with no StudyInstanceUID
    si_len0 = len(si)
    si = si.loc[~(si['StudyInstanceUID'] == ''), :].copy()
    log2both.info(f"Removed {si_len0 - len(si)} rows with no StudyInstanceUID.")

    # Ignore studies with no targets
    si_len0 = len(si)
    si = si.loc[si[target_counter] > 0, :].copy()
    log2both.info(f"Removed {si_len0 - len(si)} rows with no related {targets}.")

    # Sum the number of targets for each unique StudyInstanceUID (removing duplicates in the process).
    si_len0 = len(si)
    my_cols = ['StudyInstanceUID', 'NumberOfStudyRelatedSeries']
    if target == 'image':
        my_cols.append('NumberOfStudyRelatedInstances')
    si = si[my_cols].groupby('StudyInstanceUID').sum().reset_index()
    log2both.info(f"Removed {si_len0 - len(si)} rows with duplicate StudyInstanceUID.")
    log2both.info(f"Found {len(si)} valid studies.")

    si['study_counter'] = 1
    si['queried'] = False
    si['query_pass'] = None
    si['query_chunk'] = None
    si['status'] = 'pending'

    pass_num = 0
    queried_single_studies = False
    abort_run = False
    while not queried_single_studies and not si['queried'].all() and not abort_run:
        pass_start = time()
        log2both.info("")
        log2both.info("----------------------------------------------------------")
        log2both.info(f"Starting pass {pass_num}")

        si_unqueried = si.loc[~si['queried'], :].copy()

        if pass_num > 0:
            log2both.info(f"Unqueried studies: {len(si_unqueried)}")
        si_unqueried['query_pass'] = pass_num

        # Chunk the dataframe
        pass_chunk_size = int(q.chunk_size / (2 ** pass_num))
        log2both.info(f"Chunking {si_unqueried.shape[0]} studies into chunks of about {pass_chunk_size} {targets}.")
        si_unqueried['target_cumsum'] = si_unqueried[target_counter].cumsum()
        si_unqueried['chunk'] = si_unqueried['target_cumsum'] // pass_chunk_size
        my_agg: dict[str, Any] = {'StudyInstanceUID': '\\'.join, 'NumberOfStudyRelatedSeries': "sum", 'study_counter': "sum"}
        if target == 'image':
            my_agg['NumberOfStudyRelatedInstances'] = "sum"
        si_chunked = si_unqueried.groupby('chunk').agg(my_agg).reset_index()
        # Fill in constant, default values not included in groupby
        si_chunked['queried'] = False
        si_chunked['status'] = 'pending'
        studies_per_chunk = si_chunked['study_counter'].mean()
        targets_per_chunk = si_chunked[target_counter].mean()

        # Query chunks or query single studies
        if studies_per_chunk >= q.min_studies_per_chunk:
            log2both.info(f"Created {len(si_chunked)} chunks with average {studies_per_chunk:.1f} studies and "
                          f"{targets_per_chunk:.1f} {targets} each.")

            q.setup_pass(pass_si=si_chunked, pass_num=pass_num, chunked=True)
            query_dataframe(q)

            if si_chunked['queried'].any():
                si_unqueried['queried'] = si_unqueried['chunk'].isin(si_chunked.loc[si_chunked['queried'], 'chunk'])
                si_unqueried.loc[si_unqueried['queried'], 'query_chunk'] = si_unqueried.loc[
                    si_unqueried['queried'], 'chunk']
                queried_study_chunks = si_unqueried.loc[si_unqueried['queried'], 'chunk']
                # Update status columns (probably 'Success' except for failures in final pass)
                queried_chunk_statuses =  si_chunked.loc[si_chunked['queried'], ['chunk', 'status']].set_index('chunk')
                # Use map not merge because merge resets index
                queried_study_statuses = queried_study_chunks.map(queried_chunk_statuses['status']).rename('status')
                si_unqueried.update(queried_study_statuses)
            log2both.info(f"Pass {pass_num} successfully queried {si_chunked['queried'].sum()} of "
                          f"{len(si_chunked)} chunks.")
            log2both.info(f"Pass {pass_num} successfully queried {si_unqueried['queried'].sum()} of "
                          f"{len(si_unqueried)} studies.")
            if si_chunked['queried'].sum() < len(si_chunked):
                log2both.info("Chunk status count:")
                log2both.info(si_chunked['status'].value_counts())

            if q.status.lower() == 'association rejected':
                # The pass was aborted because association was rejected too many times.
                log2both.warning(f"Association rejected {q.query_settings.chunk_retry_limit} times. Next pass will query single studies.")
                # Set pass_num such that next pass will be single studies.
                pass_num = math.ceil(math.log2(q.chunk_size / q.min_studies_per_chunk)) - 1  # Will add 1 before next iter
                log2both.info(f"Setting next pass number to {pass_num + 1} to make next pass query single studies.")
        else:
            log2both.info(f"Querying single studies because chunking yielded average {studies_per_chunk:.1f} studies "
                          f"per chunk, which is less than the minimum of {q.min_studies_per_chunk}.")
            si_unqueried.drop(columns=['chunk'], inplace=True)

            q.setup_pass(pass_si=si_unqueried, pass_num=pass_num, chunked=False)
            query_dataframe(q)

            queried_single_studies = True
            si_unqueried['query_chunk'] = None  # Should already be None
            abort_run = q.status.lower() == 'association rejected'
            log2both.info(f"Pass {pass_num} successfully queried {si_unqueried['queried'].sum()} of {len(si_unqueried)}"
                          f" studies.")
            if si_unqueried['queried'].sum() < len(si_unqueried):
                log2both.info("Study status count:")
                log2both.info(si_unqueried['status'].value_counts())

        si.update(si_unqueried[['queried', 'query_pass', 'query_chunk', 'status']])
        # NOTE: `astype(bool)` should not be needed in pandas>=3
        # https://github.com/pandas-dev/pandas/issues/55509
        # It seems to be fixed in pandas>=2.2.0
        si['queried'] = si['queried'].astype(bool)

        # Log a running total if this is not the last pass
        if not (si['queried'].all() or queried_single_studies or abort_run):
            print(f"Overall successfully queried {si['queried'].sum()} of {len(si)} studies.")

        pass_end = time()

        log2both.info("Pass complete.")
        log2both.info("")
        log2both.info("| Pass Number | Chunk Size | Studies queried | Studies remaining | Start Time | End Time | "
                      "Duration (s) |")
        log2both.info("| --- | --- | --- | --- | --- | --- | --- |")
        log2both.info(f"| {pass_num} | {pass_chunk_size} | {si['queried'].sum()} | {len(si) - si['queried'].sum()} | "
                      f"{my_time(pass_start)} | {my_time(pass_end)} | {pass_end - pass_start:.0f} |")

        pass_num += 1

    # Wrap up!
    run_end = time()
    if abort_run:
        log2both.warning(f"Aborted run after association rejected {q.query_settings.study_retry_limit} times.")
    else:
        log2both.info("All passes completed.")
    log2both.info("")
    log2both.info("-----------------------------------------------------------")

    n_queried = si['queried'].sum()
    pct_queried = n_queried / len(si) * 100
    log2both.info(f"All passes successfully queried {n_queried} of {len(si)} studies ({pct_queried:.1f}%).")
    log2both.info(f"Run duration: {strftime('%H:%M:%S', localtime(run_end - run_start))}")
    log2both.info(f"Single studies {'were' if queried_single_studies else 'were not'} queried.")
    if n_queried < len(si):
        log2both.info("Study status counts:")
        log2both.info(si['status'].value_counts())
    print("----------------------------------------------------------")
    print("")

    si.to_parquet(q.output_dir / 'study_index_queried.parquet')

    if q.stat_files:
        log2both.info("Parsing query stats files.")
        stats_json_to_dataframe(q.stat_files).to_csv(q.output_dir / f'{target}_query_stats.csv', index=False)
    if q.data_files:
        log2both.info("Concatenating DICOM query responses to study index.")
        response_index = concatenate_saved_dataframes(q.data_files)
        index_file = q.output_dir / f'{target}_index.parquet'
        log2both.info(f"Saving study index to {index_file}.")
        try:
            response_index.to_parquet(index_file)
        except Exception as e:
            log2both.warning(f"Error saving {index_file}: {e}")
            index_file_pickle = index_file.with_suffix('.pkl')
            log2both.info(f"Saving as pickle to {index_file_pickle}.")
            response_index.to_pickle(index_file_pickle)


if __name__ == '__main__':
    # Future: replace argparse with click
    parser = argparse.ArgumentParser(prog='query.py', description="Query PACS in batches.")
    subparsers = parser.add_subparsers(help='Query type (by date or by study UID)', dest='query_type')

    # create the parser for the "by_date" command
    parser_by_date = subparsers.add_parser('by_date', help='Query studies by date range')
    parser_by_date.add_argument(
        '--start_date',
        required=True,
        type=str,
        help='Start date in datetime-like format.'
    )
    parser_by_date.add_argument(
        '--end_date',
        required=True,
        type=str,
        help='End date in datetime-like format.'
    )
    parser_by_date.add_argument(
        '--output_dir',
        required=True,
        type=Path,
        help='Output directory for query results.'
    )
    parser_by_date.add_argument(
        '--base_query',
        type=Path,
        help='DICOM file defining query.'
    )
    parser_by_date.add_argument(
        '--overwrite',
        action='store_true',
        help='If true, delete existing output, otherwise attempt to resume, skipping existing output.'
    )

    # create the parser for the "by_study" command
    parser_by_study = subparsers.add_parser('by_study', help='Query series or images from a table of study instance UIDs')
    parser_by_study.add_argument(
        '--study_index',
        required=True,
        type=Path,
        help='Pandas DataFrame in parquet format containing study metadata.'
    )
    parser_by_study.add_argument(
        '--output_dir',
        required=True,
        help='Directory to save series query DICOM files.'
    )
    query_group = parser_by_study.add_mutually_exclusive_group(required=False)
    query_group.add_argument(
        '--query_level',
        type=str.lower,
        help='Query level (series or image). Required if --base_query not provided.',
        choices=['series', 'image']
    )
    query_group.add_argument(
        '--base_query',
        help='DICOM file defining base query. Required if --query_level not provided.'
    )
    parser_by_study.add_argument(
        '--chunk_size',
        default=300,
        type=int,
        help='Number of series or images to query at a time. [300]'
    )
    parser_by_study.add_argument(
        '--min_studies_per_chunk',
        default=10,
        type=int,
        help='Minimum number of studies per chunk (below which query studies individually). [10]'
    )

    main_args = parser.parse_args()

    # Load configuration
    script_dir = Path(__file__).parent
    config_file = script_dir / 'query_config.toml'
    with open(config_file, 'rb') as f:
        config = tomllib.load(f)
    pacs_settings = PACSSettings(**config['pacs'])
    query_settings = QuerySettings(**config.get('query', {}))

    # Set up application entity object
    ae = AE()
    ae.add_requested_context(StudyRootQueryRetrieveInformationModelFind)

    log_start = time()
    timestamp = strftime('%Y%m%d_%H%M%S', localtime(log_start))
    od = Path(main_args.output_dir).resolve()
    if not od.exists():
        od.mkdir(parents=True)
    run_log_file = od / f'run_{timestamp}.log'
    setup_logger(log_file=run_log_file)
    log2file, log2stdout, log2both = get_loggers()

    if main_args.query_type == 'by_date':
        if main_args.base_query:
            base_query = dcmread(main_args.base_query)
            if not hasattr(base_query, 'QueryRetrieveLevel') or base_query['QueryRetrieveLevel'].value.lower() != 'study':
                log_raise(ValueError, "Provided base query DICOM file does not have QueryRetrieveLevel of 'STUDY'.")
        else:
            base_query = make_study_query()

        # Handle overwrite from both config and command-line
        if main_args.overwrite:
            query_settings.overwrite = True

        # Create QueryRunDate with settings
        date_run = QueryRunDate(
            pacs_settings=pacs_settings,
            query_settings=query_settings,
            ae=ae,
            output_dir=od,
            base_query=base_query,
            start_date=main_args.start_date,
            end_date=main_args.end_date,
        )

        query_studies_by_date(date_run)

    elif main_args.query_type == 'by_study':
        if not main_args.query_level and not main_args.base_query:
            parser_by_study.error("Must provide either --query_level or --base_query")
        if main_args.base_query:
            base_query = dcmread(main_args.base_query)
            if hasattr(base_query, 'QueryRetrieveLevel'):
                if base_query['QueryRetrieveLevel'].value.lower() not in ['series', 'image']:
                    log_raise(ValueError, "Provided base query DICOM file has invalid QueryRetrieveLevel (must be 'SERIES' or 'IMAGE').")
            else:
                log_raise(ValueError, "Provided base query DICOM file does not have QueryRetrieveLevel attribute.")
        else:
            if main_args.query_level == 'series':
                # Make base series query
                base_query = make_series_query()
            elif main_args.query_level == 'image':
                # Make base image query
                base_query = make_image_query()
            else:
                # argparse should prevent this from happening
                raise ValueError(f"Unknown query level (must be 'series' or 'image'): {main_args.query_level}")

        # Create QueryRunStudy with settings
        study_run = QueryRunStudy(
            pacs_settings=pacs_settings,
            query_settings=query_settings,
            ae=ae,
            output_dir=od,
            base_query=base_query,
            study_index=main_args.study_index,
            chunk_size=main_args.chunk_size,
            min_studies_per_chunk=main_args.min_studies_per_chunk,
        )

        query_by_study(study_run)

    else:
        # argparse should prevent this from happening
        raise ValueError(f"Unknown query type (must be 'by_date' or 'by_study'): {main_args.query_type}")
    
