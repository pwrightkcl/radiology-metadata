import argparse
from os import getenv
import os
from pathlib import Path

from pynetdicom import AE, evt, AllStoragePresentationContexts, debug_logger
from pynetdicom.sop_class import Verification
from pydicom.uid import generate_uid


# Handler for incoming C-STORE requests
def handle_store(event, output_dir, save_pixels=False):
    try:
        ds = event.dataset
        ds.file_meta = event.file_meta

        # Get study and series UIDs
        study_uid = getattr(ds, 'StudyInstanceUID', f'unknown_{generate_uid()}')
        series_uid = getattr(ds, 'SeriesInstanceUID', f'unknown_{generate_uid()}')

        # Create directory path based on study and series UIDs
        series_dir = output_dir / study_uid / series_uid
        series_dir.mkdir(parents=True, exist_ok=True)

        # Build full file path
        filename = f"{ds.SOPInstanceUID}.dcm"
        file_path = os.path.join(series_dir, filename)

        if not save_pixels:
            # Remove pixel data
            if 'PixelData' in ds:
                del ds.PixelData

        # Save the DICOM file
        ds.save_as(file_path, write_like_original=False)

        return 0x0000  # Success

    except Exception as e:
        print(f"Error storing DICOM file:\n{str(e)}")
        return 0xC210  # Processing failure


def handle_assoc(event):
    calling_ae = event.assoc.requestor.ae_title.strip()
    if calling_ae not in allowed_aets:
        print(f"Rejected AE Title: {calling_ae}")
        return 0x0122  # Calling AE not recognized
    print(f"Accepted AE Title: {calling_ae}")
    return 0x0000


def run_storescp(output_dir, save_pixels=False):
    print(f"Starting storeSCP to save image to {output_dir}")

    # Create AE and add presentation contexts
    ae = AE(ae_title=aet)  # , maximum_pdu_size=DEFAULT_MAX_PDU)
    for context in AllStoragePresentationContexts:
        ae.add_supported_context(context.abstract_syntax)

    ae.add_supported_context(Verification)

    # Event handlers
    handlers = [
        (evt.EVT_C_STORE, handle_store, [output_dir, save_pixels]),
        (evt.EVT_ACCEPTED, handle_assoc)
    ]

    ae.require_calling_aet = allowed_aets

    try:
        scp = ae.start_server(("0.0.0.0", local_port), evt_handlers=handlers, block=True)
    except KeyboardInterrupt:
        print("Keyboard interrupt. Closing storeSCP.")
        scp.shutdown()
        exit(130)


if __name__ == '__main__':
    this_description = """Set up a storeSCP instance to save incoming DICOM images from PACS.
    Requires environment variables PACS_IP, PACS_PORT, AEC, AET, and LOCAL_PORT to be set."""
    this_description = '\n'.join([line.strip() for line in this_description.split('\n')])
    parser = argparse.ArgumentParser(description=this_description)
    parser.add_argument(
        '--output_dir',
        required=True,
        help='Directory to save image files.',
        type=Path
    )
    parser.add_argument(
        '--save_pixel_data',
        action='store_true',
        help='Save pixel data in the DICOM files (default: save only metadata).'
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

    allowed_aets = [aec, aet]

    debug_logger()

    run_storescp(output_dir=main_args.output_dir, save_pixels=main_args.save_pixel_data)
