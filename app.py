from flask import Flask, render_template, send_file, request
from intern.convenience import array
import numpy as np
import zarr
import math
import tempfile
import shutil

app = Flask(__name__)

@app.route("/")
def home():
    return render_template("home.html")

@app.route("/download")
def download():

    # TODO: add try/accept here that reloads html page with error message

    # Define subvolume bounds. End slices are excluded
    x_start = int(request.args.get('x-start'))
    x_end = int(request.args.get('x-end'))
    y_start = int(request.args.get('y-start'))
    y_end = int(request.args.get('y-end'))
    z_start = int(request.args.get('z-start'))
    z_end = int(request.args.get('z-end'))
    # TODO: add metadata call here to verify bounds

    # TODO: add this to the web form
    mip = 0

    # Calculate extents for readability later
    x_extent = x_end - x_start
    y_extent = y_end - y_start
    z_extent = z_end - z_start

    # Set S3 paths to data
    em = array(request.args.get('image-path'))
    # if em.layer_type != 'image':
    #     raise ValueError("The given S3 image path is invalid or contains data other than images.")
    # TODO: need to implement as three dropdowns
    # TODO: need to make metadata api call to confirm image layer

    seg = False # pull only EM; set to True to pull segmentation too
    if request.args.get('seg-path'):
        seg = True
        seg_vol = array(request.args.get('seg-path'))
        # if seg_vol.layer_type != 'segmentation':
        #     raise ValueError("The given S3 segmentation path is invalid or contains data other than segmentation.")
        # TODO: need to make metadata api call to confirm seg layer

    # Set output dir name
    output_dirname = request.args.get('downloaded-filename')

    ##########
    ## CODE ##
    ##########
    ZARR_CHUNK_SIZE = [32, 256, 256]

    with tempfile.TemporaryDirectory() as tmpdirname:

        # Calculate shape from extent and resolution
        em_res = list(em.voxel_size)[::-1]
        if seg:
            seg_res = list(seg_vol.voxel_size)[::-1]

        store = zarr.DirectoryStore(tmpdirname)
        zarr_group = zarr.group(store, overwrite=True)
        chunk_size = ZARR_CHUNK_SIZE
        if chunk_size[0] > z_extent: chunk_size[0] = z_extent
        if chunk_size[1] > y_extent: chunk_size[1] = y_extent
        if chunk_size[2] > x_extent: chunk_size[2] = x_extent

        em_array = zarr_group.create_dataset(
            'raw', 
            shape=[z_extent, y_extent, x_extent], 
            chunks=chunk_size, 
            dtype=em.dtype,
        )
        if seg:
            seg_array = zarr_group.create_dataset(
                'seg', 
                shape=[z_extent, y_extent, x_extent], 
                chunks=chunk_size, 
                dtype=seg_vol.dtype,
            )

        # Function to get x or y or z coords for chunks
        def get_chunks_1d(start_coord, chunk_size, extent):
            num_batches = math.ceil(extent / chunk_size)
            start_indices = [int(start_coord + a*chunk_size) for a in range(num_batches)]
            stop_indices = [int(start_coord + a*chunk_size + chunk_size) for a in range(num_batches)]
            stop_indices[-1] = int(start_coord + extent)
            chunks = list(zip(start_indices, stop_indices))
            return chunks

        batch_size = 2048 # this is arbitrary, just needs to fit in memory
        x_chunks = get_chunks_1d(x_start, batch_size, x_extent)
        y_chunks = get_chunks_1d(y_start, batch_size, y_extent)
        z_chunks = get_chunks_1d(z_start, batch_size, z_extent)

        # Pull the cutouts and add to the zarr
        for x_chunk in x_chunks:
            for y_chunk in y_chunks:
                for z_chunk in z_chunks:
                    em_cutout = em[z_chunk[0]:z_chunk[1], y_chunk[0]:y_chunk[1], x_chunk[0]:x_chunk[1]]
                    em_array[z_chunk[0]-z_start:z_chunk[1]-z_start, y_chunk[0]-y_start:y_chunk[1]-y_start, x_chunk[0]-x_start:x_chunk[1]-x_start] = em_cutout

                    if seg:
                        seg_cutout = seg_vol[z_chunk[0]:z_chunk[1], y_chunk[0]:y_chunk[1], x_chunk[0]:x_chunk[1]]
                        seg_array[z_chunk[0]-z_start:z_chunk[1]-z_start, y_chunk[0]-y_start:y_chunk[1]-y_start, x_chunk[0]-x_start:x_chunk[1]-x_start] = seg_cutout

        em_array.attrs["resolution"] = em_res
        em_array.attrs["offset"] = (z_start, y_start, x_start)
        if seg: seg_array.attrs["resolution"] = seg_res

        shutil.make_archive(tmpdirname, 'zip', tmpdirname)

        return send_file(
            f"{tmpdirname}.zip",
            mimetype='application/zip',
            as_attachment=True,
            download_name=f'{output_dirname}.zarr.zip'
        )