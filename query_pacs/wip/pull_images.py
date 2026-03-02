import argparse
from os import getenv
from time import sleep, time, localtime, strftime
from pathlib import Path
import json
import warnings

from tqdm import tqdm
import pandas as pd
from pydicom.dataset import Dataset
from pynetdicom.status import code_to_category
from pynetdicom import AE, evt, StoragePresentationContexts, debug_logger
from pynetdicom.sop_class import Verification, StudyRootQueryRetrieveInformationModelMove


def my_time(time_in_seconds):
    """Convert seconds from epoch to string for logging."""
    return strftime('%Y-%m-%d %H:%M:%S', localtime(time_in_seconds))


def pull_images(image_index, output_dir, chunk_size=1, retry_limit=10):
    """Pull images from PACS from an image index.
    https://pydicom.github.io/pynetdicom/dev/examples/qr_move.html
    """
    # Uncomment this line for debugging output from pynetdicom
    # debug_logger()

    run_start = time()

    # Load image index
    image_index = pd.read_parquet(image_index)
    # Check image index contains SOPInstanceUID
    if 'SOPInstanceUID' not in image_index.columns:
        raise ValueError("Image index does not contain SOPInstanceUID column.")

    # Check if output directory exists, if not create it
    output_dir = Path(output_dir)
    if not output_dir.exists():
        output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = strftime('%Y%m%d_%H%M%S', localtime(run_start))
    run_log_file = output_dir / f'run_{timestamp}.log'
    with run_log_file.open('w') as run_log:
        run_log.write("Pulling images.\n")
        run_log.write(f"Run start: {my_time(run_start)}\n")

    # # Implement the handler for evt.EVT_C_STORE
    # def handle_store(event):
    #     """Handle a C-STORE request event."""
    #     this_ds = event.dataset
    #     this_ds.file_meta = event.file_meta
    #
    #     # Save the dataset using the SOP Instance UID as the filename
    #     this_ds.save_as(Path(output_dir) / this_ds.SOPInstanceUID + '.dcm', write_like_original=False)
    #
    #     # Return a 'Success' status
    #     return 0x0000
    #
    # handlers = [(evt.EVT_C_STORE, handle_store)]

    # Initialise the Application Entity
    ae = AE(ae_title=aet)

    # Add a requested presentation context
    ae.add_requested_context(Verification)
    ae.add_requested_context(StudyRootQueryRetrieveInformationModelMove)

    # Verify the store SCP is running
    assoc = ae.associate(localhost, local_port, ae_title=aet)
    if assoc.is_established:
        # Send a C-ECHO request
        status = assoc.send_c_echo()

        # Release the association
        assoc.release()

        if status:
            if status.Status == 0:
                print("Local storeSCP is running.")
            else:
                print("Could not verify store SCP is running.")
                print(f"Status: 0x{status.Status:04x} ({code_to_category(status.Status)})")
                exit(1)
        else:
            print('Connection timed out, was aborted or received invalid response')
            print('Could not verify store SCP is running')
            exit(1)

    else:
        print('Association rejected, aborted or never connected')
        print('Could not verify store SCP is running')
        exit(1)
    with run_log_file.open('a') as run_log:
        run_log.write(f"{my_time(time())}: Local store SCP is running.\n")

    # # Add the Storage SCP's supported presentation contexts
    # ae.supported_contexts = StoragePresentationContexts

    # Start our Storage SCP in non-blocking mode, listening on local port
    # ae.ae_title = aet
    # scp = ae.start_server(("127.0.0.1", local_port), block=False, evt_handlers=handlers)

    # Chunk the SOPInstanceUIDs into groups of chunk_size
    sop_uid_chunks = image_index['SOPInstanceUID'].tolist()
    sop_uid_chunks = [sop_uid_chunks[i:i + chunk_size] for i in range(0, len(sop_uid_chunks), chunk_size)]
    n_chunks = len(sop_uid_chunks)

    bar_desc = ("Pulling images" if chunk_size == 1 else f"Pulling chunks of {chunk_size} images")
    bar = tqdm(total=image_index.shape[0], desc=bar_desc, unit="images")
    for chunk, sop_uids in enumerate(sop_uid_chunks):
        ds = Dataset()
        ds.QueryRetrieveLevel = 'IMAGE'
        # ds.StudyInstanceUID = row['StudyInstanceUID']
        ds.SOPInstanceUID = sop_uids

        tries = 0
        chunk_status = 'pending'
        while tries < retry_limit and chunk_status != 'success':
            try_start = time()
            assoc = ae.associate(pacs_ip, pacs_port, ae_title=aec)
            if assoc.is_established:
                # Use the C-MOVE service to send the identifier
                responses = assoc.send_c_move(ds, aet, StudyRootQueryRetrieveInformationModelMove)
                for (status, identifier) in responses:
                    if status:
                        if status.Status == 0x0000:
                            chunk_status = 'success'
                        else:
                            chunk_status = code_to_category(status.Status)
                    else:
                        chunk_status = 'query failure'
                assoc.release()
            else:
                chunk_status = 'association failure'
            try_end = time()
            with run_log_file.open('a') as run_log:
                if chunk_size == 1:
                    run_log.write(f"Image {chunk} try {tries} status {chunk_status} duration {try_end - try_start:.2f}\n")
                else:
                    run_log.write(f"Chunk {chunk} images {len(sop_uids)} try {tries} status {chunk_status} duration {try_end - try_start:.2f}\n")
            tries += 1

        if tries >= retry_limit:
            print(f"Failed to pull images after {retry_limit} tries. Aborting run.")
            with run_log_file.open('a') as run_log:
                run_log.write(f"{my_time(time())}: Failed to pull images after {retry_limit} tries. Aborting run.\n")
            break

        bar.update(len(sop_uids))

        # # Stop our Storage SCP
        # scp.shutdown()

    run_end = time()
    with run_log_file.open('a') as run_log:
        run_log.write(f"Run end: {my_time(run_end)}\n")
        run_duration = run_end - run_start
        run_duration_str = strftime('%H:%M:%S', localtime(run_duration))
        run_log.write(f"Run duration: {run_duration_str}\n")


if __name__ == '__main__':
    this_description = """Pull images from an index.
    Requires environment variables PACS_IP, PACS_PORT, AEC, AET, and LOCAL_PORT to be set."""
    this_description = '\n'.join([line.strip() for line in this_description.split('\n')])
    parser = argparse.ArgumentParser(description=this_description)
    parser.add_argument(
        '--image_index',
        required=True,
        help='Pandas dataframe in parquet format containing image metadata.'
    )
    parser.add_argument(
        '--output_dir',
        required=True,
        type=Path,
        help='Directory to save image files.'
    )
    parser.add_argument(
        '--chunk_size',
        default=1,
        type=int,
        help='Number of instances to pull at a time. [1]'
    )
    parser.add_argument(
        '--retry_limit',
        default=10,
        type=int,
        help='Number of times to retry pull if rejected. [10]'
    )

    main_args = parser.parse_args()

    # PACS setup
    # get environment variables for PACS setup
    pacs_ip = getenv('PACS_IP')
    pacs_port = getenv('PACS_PORT')
    aec = getenv('AEC')
    aet = getenv('AET')
    local_port = getenv('LOCAL_PORT')

    # check none of the environment variables were empty
    if not all([pacs_ip, pacs_port, aec, aet, local_port]):
        raise ValueError("One or more of the PACS environment variables are not set.")
    pacs_port = int(pacs_port)
    local_port = int(local_port)

    localhost = '127.0.0.1'

    pull_images(image_index=main_args.image_index, output_dir=main_args.output_dir,
                retry_limit=main_args.retry_limit, chunk_size=main_args.chunk_size)
