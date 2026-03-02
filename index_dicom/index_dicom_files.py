"""Read selected tags from every DICOM (.dcm) file and save as a CSV and parquet.

Set chunk_size to save the results at set intervals.
Specify dicom_tags to retrieve using keywords, e.g. Series Description (0008,103E) is 'SeriesDescription'.
https://dicom.innolitics.com/ciods/mr-image/general-series/0008103e
Requires pyarrow to save parquet files.
"""
import argparse
from pathlib import Path
import csv
from math import floor, ceil
import gc
from datetime import timedelta
import warnings

import pandas as pd
from pydicom import dcmread, Dataset
from pydicom.errors import InvalidDicomError
from pydicom.datadict import keyword_for_tag, dictionary_VR, dictionary_VM, dictionary_has_tag
from pydicom.multival import MultiValue
from pydicom.uid import UID
from pydicom.valuerep import DSfloat, IS, PersonName
import timeit
from tqdm import tqdm
import pyarrow as pa


def main(in_dir: str, out_dir: str, fields_file: str, chunk_size: int):
    """Read selected tags from the first DICOM file in each subdirectory and save as CSV and parquet.

    Parameters:
        in_dir:         Directory containing DICOM files.
        out_dir:        Directory to save CSV and Parquet output.
        fields_file:    Text file containing list of DICOM keywords to index.
                        Default: basic study, series, and instance info.
        chunk_size:     Save output in chunks of N DICOM files. Will attempt to combine chunks at the end.

    Returns:
        None. Saves results to CSV and Parquet files in out_dir.
    """
    dicom_dir = Path(in_dir)
    output_dir = Path(out_dir)
    if not output_dir.is_dir():
        output_dir.mkdir()

    if fields_file:
        with open(fields_file, 'r') as f:
            fields = [line.rstrip() for line in f]
    else:
        fields = None

    tic_total = timeit.default_timer()
    print(f"Constructing DICOM index from {dicom_dir}.")

    dicom_files = list_dicom_files(dicom_dir, output_dir)
    n_dicom = len(dicom_files)
    if n_dicom == 0:
        print("No DICOM files found.")
        return

    if chunk_size:
        # Figure out chunk number and check for existing saved chunks.
        n_chunk, chunks_saved = chunk_init(n_dicom, chunk_size, output_dir)
        if n_chunk == 1:
            # Treat as single chunk
            chunk_size = None
    else:
        # Putting these here to avoid IDE warnings.
        n_chunk = 1
        chunks_saved = 0

    if not chunk_size:
        tic = timeit.default_timer()
        metadata = []
        for dicom_file in tqdm(dicom_files, total=n_dicom):
            metadata.append(dcm_to_tags(dicom_file=dicom_file, fields=fields))
        toc = timeit.default_timer()
        print(f"Indexed {n_dicom} DICOM files in {(toc - tic):.0f} seconds.")
        dicom_index = pd.DataFrame(metadata)
        save_tables(dicom_index, output_dir / 'dicom_index')
    else:
        # Read DICOM files in chunks
        chunk_width = len(str(n_chunk - 1))
        chunk_times = {}
        for chunk in tqdm(range(chunks_saved, n_chunk), total=n_chunk):
            tic_chunk = timeit.default_timer()

            start = chunk * chunk_size
            end = min((chunk + 1) * chunk_size, n_dicom)
            chunk_files = dicom_files[start:end]
            metadata = []
            for dicom_file in tqdm(chunk_files, total=len(chunk_files)):
                metadata.append(dcm_to_tags(dicom_file=dicom_file, fields=fields))
            dicom_index = pd.DataFrame(metadata)
            save_tables(dicom_index, output_dir / f'dicom_index_chunk{chunk:0{chunk_width}}')
            toc_chunk = timeit.default_timer()
            chunk_times[f'{chunk:0{chunk_width}}'] = toc_chunk - tic_chunk
        for n, t in chunk_times.items():
            print(f"Chunk {n}: {t:.0f} seconds.")
        concatenate_chunks(output_dir, n_chunk)
    toc_total = timeit.default_timer()
    total_delta = timedelta(seconds=(toc_total - tic_total))
    print(f"Total time: {total_delta}")


def list_dicom_files(dicom_dir: Path, output_dir: Path) -> list:
    """List all DICOM files in a directory and save to a text file.

    Parameters:
        dicom_dir: Path to directory containing DICOM files.
        output_dir: Path to directory to save the list of DICOM files.

    Returns:
        List of DICOM files.
    """
    file_list_file = output_dir / 'dicom_file_list.txt'
    if file_list_file.exists():
        with open(file_list_file, 'r') as f:
            file_list = [line.rstrip() for line in f]
        print(f"Using existing list of {len(file_list)} DICOM files.")
    else:
        print("Listing DICOM files (this may take some time).")
        tic = timeit.default_timer()
        file_list = []
        with file_list_file.open('w') as f:
            for d in dicom_dir.glob('**/*.dcm'):
                f.write(str(d) + '\n')
                file_list.append(str(d))
        toc = timeit.default_timer()
        print(f"Listed {len(file_list)} DICOM files in {(toc - tic):.2f} seconds.")
    return file_list


def chunk_init(n_dicom, chunk_size, output_dir) -> tuple[int, int]:
    """Calculate number of chunks and check for existing saved chunks.

    Parameters:
        n_dicom: Number of DICOM files.
        chunk_size: Number of DICOM files to save in each chunk.
        output_dir: Directory to save the chunks.

    Returns:
        Number of chunks.
        Number of chunks already saved.
    """
    n_chunk = ceil(n_dicom / chunk_size)
    chunk_width = len(str(n_chunk - 1))
    if n_chunk == 1:
        chunks_saved = None
    else:
        # Look for existing saved chunks and try to resume
        n_parquet = len([f for f in output_dir.glob('dicom_index_chunk*.parquet')])
        n_pickle = len([f for f in output_dir.glob('dicom_index_chunk*.pickle')])
        chunks_saved = n_parquet + n_pickle
        if chunks_saved > 0:
            if chunks_saved > n_chunk:
                print(f"Found {chunks_saved} files in {output_dir} matching dicom_index_chunk*.parquet but "
                      f"expected a maximum of {n_chunk}.")
                print(f"Stopping to avoid overwriting data. "
                      f"Please clean up {output_dir} or use a different out_dir.")
                exit()
            else:
                print("Found existing chunk files. Checking ...")
                for check_chunk in range(chunks_saved):
                    chunk_stem = output_dir / f'dicom_index_chunk{check_chunk:0{chunk_width}}'
                    if chunk_stem.with_suffix('.parquet').is_file():
                        pass
                    elif chunk_stem.with_suffix('.pickle').is_file():
                        pass
                    else:
                        print(f"Found {chunks_saved} files in {output_dir} matching dicom_index_chunk*.parquet but "
                              f"could not find dicom_index_chunk{check_chunk:0{chunk_width}}.parquet.")
                        print(f"Stopping to avoid overwriting data. "
                              f"Please clean up {output_dir} or use a different out_dir.")
                        exit()
                if chunks_saved == n_chunk:
                    print(f"Found all {chunks_saved} existing chunk files. Nothing to do.")
                    exit()
                print(f"Found {chunks_saved} existing chunk files. Resuming from next chunk.")
    return n_chunk, chunks_saved


def dcm_to_tags(dicom_file, fields = None):
    """Read a DICOM file and return a dictionary of DICOM attributes.
    Adds meta-attributes for filepath. Handles errors.
    Will read only specified attributes if given `fields`, otherwise will read all.

    Parameters:
        dicom_file: Path to DICOM file.
        fields: List of fields to extract from DICOM file.

    Returns:
        Dictionary of DICOM attributes.
    """
    # Filename parts
    this_data = {'dicom_filepath': str(dicom_file),
                 'warnings': None,
                 'error': None}
    # Try reading DICOM file and record error if it fails
    try:
        read_warning = None
        with warnings.catch_warnings(record=True) as w:
            ds = dcmread(dicom_file)
            # If there was a warning, save a clip of the message to later add to this_data['warning']
            if len(w) > 0:
                read_warning = 'dcmread: ' + str(w[0].message).split(':')[0]
    except InvalidDicomError as e:
        this_data['error'] = str(e)
        return this_data

    tag_warnings = []
    try:
        if fields:
            tag_dict, tag_warnings = get_dicom_tags(ds, fields)
        else:
            tag_dict, tag_warnings = get_all_dicom_tags(ds)
        this_data.update(tag_dict)
    except Exception as e:
        this_data['error'] = f"Error processing DICOM metadata: {str(e)}"

    if read_warning:
        if tag_warnings:
            this_data['warnings'] = '\n'.join([read_warning] + tag_warnings)
        else:
            this_data['warnings'] = read_warning
    elif tag_warnings:
        this_data['warnings'] = '\n'.join(tag_warnings)

    return this_data


def get_dicom_tags(ds: Dataset | Path, fields: list) -> (dict, list):
    """Read selected tags from a pydicom Dataset into a dictionary.

    Parameters:
        ds: pydicom Dataset
        fields: List of DICOM keywords to index.

    Returns:
        dicom_dict: Dictionary of DICOM tags.
        warning_clips: List of warning messages encountered while reading the DICOM file.
    """
    dicom_dict = {}
    warning_clips = []
    for field in fields:
        if field in ds:
            with warnings.catch_warnings(record=True) as w:
                this_value = ds[field].value
                # If there was a warning, save a clip of the message to later add to this_data['warning']
                if len(w) > 0:
                    clip = str(w[0].message).split(':')[0]
                    warning_clips.append(f"{field}: {clip}")
            this_vr = ds[field].VR
            this_vm = ds[field].VM
            if field in ['ImageType', 'PatientOrientation', 'ImagePositionPatient']:
                # These fields are expected to be lists.
                dicom_dict[field] = list(this_value)
            elif this_vr == 'SQ':
                # Handle sequences
                for i, item in enumerate(this_value):
                    for tag in item.keys():
                        keyword = keyword_for_tag(tag)
                        dicom_dict[f"{field}.{i}.{keyword}"] = item[tag].value
            elif this_vm > 1:
                # Other attributes with multiple values
                if field in ['SeriesDescription', 'ScanningSequence', 'ScanOptions', 'SequenceVariant']:
                    # These fields are usually single strings, so convert lists to space-separated string.
                    dicom_dict[field] = ' '.join(this_value)
                else:
                    # If in doubt, convert to string as is, this will include brackets for lists.
                    dicom_dict[field] = str(this_value)
            else:
                dicom_dict[field] = this_value
        else:
            dicom_dict[field] = None
    return dicom_dict, warning_clips


def get_all_dicom_tags(ds: Dataset, prefix: str = "") -> (dict, list):
    """Convert all the elements in a pydicom Dataset to a dictionary, flattening sequences.
    Sequence elements will be named 'SequenceKeyword.<i>.<NestedElementKeyword>'.

    Parameters:
        ds: pydicom Dataset.
        prefix (str): Prefix to key, used for nested elements.

    Returns:
        dicom_dict: Dictionary of DICOM tags.
        warning_clips: List of warning messages encountered while reading the DICOM file.
    """
    dicom_dict = {}
    warning_clips = []
    for key in ds.keys():
        if key == (0x7FE0,0x0010):
            # Ignore PixelData
            continue
        with warnings.catch_warnings(record=True) as w:
            el = ds[key]
            if ~el.is_private and el.keyword:
                _key = prefix + el.keyword
            else:
                # Make a string from the hex tag that is human-readable, usable as a Pandas column,
                # and still works as a key for indexing the Dataset object.
                _key = prefix + f'0x{el.tag:08x}'
            # If there was a warning, save a clip of the message
            if len(w) > 0:
                clip = str(w[0].message).split(':')[0]
                warning_clips.append(f"Loading {_key}: {clip}")
        if el.VR == 'SQ':
            # Handle sequences
            for i, sq_ds in enumerate(el.value):
                sq_prefix = prefix + f'{_key}.{i}.'
                sq_dict, sq_clips = get_all_dicom_tags(sq_ds, sq_prefix)
                dicom_dict.update(sq_dict)
                if sq_clips:
                    warning_clips.extend(sq_clips)
        else:
            # Handle regular elements
            with warnings.catch_warnings(record=True) as w:
                dicom_dict[_key] = _convert_value(el.value)
                # If there was a warning, save a clip of the message
                if len(w) > 0:
                    clip = str(w[0].message).split(':')[0]
                    warning_clips.append(f"Converting {_key}: {clip}")
    if warning_clips:
        dicom_dict['warning'] = '\n'.join(warning_clips)
    return dicom_dict, warning_clips


def _convert_value(v):
    # https://github.com/pydicom/pydicom/discussions/1673#discussioncomment-3417403
    t = type(v)
    if t in (int, float):
        cv = v
    elif t == DSfloat:
        cv = float(v)
    elif t == IS:
        cv = int(v)
    elif t in (list, MultiValue):
        cv = [_convert_value(mv) for mv in v]
    elif t in (str, PersonName, UID):
        cv = _sanitise_unicode(str(v))
        if len(cv) > 256:
            cv = cv[:253] + '...'
    elif t == bytes:
        s = v.decode('ascii', 'replace')
        cv = _sanitise_unicode(s)
    else:
        cv = repr(v)
    return cv


def _sanitise_unicode(s):
    return s.replace(u"\u0000", "").strip()


def save_tables(dicom_index: pd.DataFrame, file_stem: Path):
    """Save the DICOM index as a CSV and parquet file.
    If there is an error saving to parquet, save as pickle and write an error file.

    Parameters:
        dicom_index: DataFrame containing the DICOM index.
        file_stem: Path to the output file stem.

    Returns:
        None. Saves the CSV and parquet files.
    """
    dicom_index.to_csv(file_stem.with_suffix('.csv'), index=False, quoting=csv.QUOTE_NONNUMERIC)

    # Try to sanitize table so that columns with mostly int or float cells with a few cells that are str with value 'None' are converted to pd.NA.
    empty_lists = pd.Series([[]] * len(dicom_index), index=dicom_index.index)
    for col in dicom_index.columns:
        try:
            pa.Array.from_pandas(dicom_index[col])
        except Exception as e:
            if dictionary_has_tag(col):
                vr = dictionary_VR(col)
                vm = dictionary_VM(col)
                if vm == '1':
                    if vr in ['DS', 'IS', 'FL', 'FD', 'OF', 'SL', 'SS', 'UL', 'US']:
                        # Convert to numeric if VR is numeric and VM is 1
                        dicom_index[col] = pd.to_numeric(dicom_index[col], errors='coerce')
                    else:
                        # Convert to string if VR is not numeric
                        dicom_index[col] = dicom_index[col].fillna('')
                        dicom_index[col] = dicom_index[col].astype(str)
                        dicom_index[col] = dicom_index[col].replace('[]', '')
                else:
                    # Attribute can contain multiple values.
                    # Replace empty strings with NaN.
                    dicom_index[col] = dicom_index[col].replace('', pd.NA)
                    # Replace NaN with empty list.
                    dicom_index[col] = dicom_index[col].fillna(empty_lists)
                    # Convert to list if VM is not 1 and cell is not already a list
                    dicom_index[col] = dicom_index[col].apply(lambda x: x if isinstance(x, list) else [x])
            else:
                try_numeric = pd.to_numeric(dicom_index[col], errors='coerce')
                if try_numeric.notna().sum() / len(dicom_index) > 0.5:
                    # If the majority of the values can be converted to numeric, convert the column to numeric
                    dicom_index[col] = try_numeric
                elif dicom_index[col].apply(lambda x: isinstance(x, list)).sum() > 0.5:
                    # If the majority of the values are lists, convert the column to list
                    # Replace empty strings with NaN.
                    dicom_index[col] = dicom_index[col].replace('', pd.NA)
                    # Replace NaN with empty list.
                    dicom_index[col] = dicom_index[col].fillna(empty_lists)
                    # Convert to list if VM is not 1 and cell is not already a list
                    dicom_index[col] = dicom_index[col].apply(lambda x: x if isinstance(x, list) else [x])
                else:
                    # Otherwise, convert the column to string
                    dicom_index[col] = dicom_index[col].fillna('')
                    dicom_index[col] = dicom_index[col].astype(str)
                    dicom_index[col] = dicom_index[col].replace('[]', '')

    # Try saving to parquet and save to pickle if it fails.
    try:
        dicom_index.to_parquet(file_stem.with_suffix('.parquet'))
    except Exception as e:
        print(f"Error saving to parquet, saving as pickle instead:\n{e}")
        dicom_index.to_pickle(file_stem.with_suffix('.pickle'))
        with file_stem.with_suffix('.parquet.error').open('w') as f:
            f.write(str(e))


def concatenate_chunks(output_dir: Path, n_chunk: int):
    """Attempt to concatenate saved chunks into a single index.

    Parameters:
        output_dir: Directory containing the saved chunks.
        n_chunk: Number of chunks to concatenate.

    Returns:
        None. Saves the concatenated index.
    """
    print("Attempting to concatenate chunks.")
    tic = timeit.default_timer()
    chunk_width = len(str(n_chunk - 1))
    df_list = []
    missing_chunks = []
    for chunk in tqdm(range(n_chunk), total=n_chunk):
        chunk_stem = output_dir / f'dicom_index_chunk{chunk:0{chunk_width}}'
        if chunk_stem.with_suffix('.parquet').is_file():
            dicom_index = pd.read_parquet(chunk_stem.with_suffix('.parquet'))
            df_list.append(dicom_index)
        elif chunk_stem.with_suffix('.pickle').is_file():
            dicom_index = pd.read_pickle(chunk_stem.with_suffix('.pickle'))
            df_list.append(dicom_index)
        else:
            missing_chunks.append(chunk)
    dicom_index = pd.concat(df_list)
    save_tables(dicom_index, output_dir / 'dicom_index')
    toc = timeit.default_timer()
    print(f"Concatenated {n_chunk - len(missing_chunks)} chunks in {toc - tic:.1f} seconds.")
    if len(missing_chunks) > 0:
        print("Could not find every chunk. Concatenated index will be saved without these chunks:")
        print(missing_chunks)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Index selected tags from the each .dcm file. Will attempt to resume "
                                                 "if saved chunks exist in `out_dir`.")
    parser.add_argument(
        '--in_dir',
        required=True,
        help='Root directory containing DICOM files.'
    )
    parser.add_argument(
        '--out_dir',
        required=True,
        help='Directory to save CSV and Parquet output.'
    )
    parser.add_argument(
        '--fields_file',
        required=False,
        help='Text file containing DICOM keywords to be indexed.'
    )
    parser.add_argument(
        '--chunk_size',
        required=False,
        type=int,
        help='Save output in chunks of N DICOM files. Will attempt to combine chunks at the end.'
    )
    main_args = parser.parse_args()
    main(in_dir=main_args.in_dir, out_dir=main_args.out_dir, fields_file=main_args.fields_file,
         chunk_size=main_args.chunk_size)
