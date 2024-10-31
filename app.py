from flask import Flask, render_template, send_file
from cloudvolume import CloudVolume
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

    # Define your subvolume bounds. End slices are excluded
    x_end = 120692
    x_start = x_end - 2000
    y_end = 103648
    y_start = y_end - 2000
    z_end = 21365
    z_start = z_end - 2
    mip = 0

    # Calculate extents for readability later
    x_extent = x_end - x_start
    y_extent = y_end - y_start
    z_extent = z_end - z_start

    # Set S3 paths to data
    em = CloudVolume('s3://bossdb-open-data/iarpa_microns/minnie/minnie65/em', mip=mip, parallel=True, progress=True)
    seg = False # pull only EM; set to True to pull segmentation too
    seg_vol = CloudVolume('s3://bossdb-open-data/iarpa_microns/minnie/minnie65/seg', mip=mip, parallel=True, progress=True)

    # Set output dir name
    output_dirname = "minnie65_cutout_test_flask"

    # A segmentation volume pulled this way takes a really long time to convert to contours.
    # Like, my laptop CPU took 30+ minutes to finish a 1.5 gb volume and nearly overheated.
    # I think PyReconstruct is just not designed for dense annotations.
    # Could consider bringing this up to Michael and Julian.

    ##########
    ## CODE ##
    ##########
    with tempfile.TemporaryDirectory() as tmpdirname:

        # Calculate shape from extent and resolution
        em_res = list(em.resolution)[::-1]
        if seg:
            seg_res = list(seg_vol.resolution)[::-1]

        store = zarr.DirectoryStore(tmpdirname)
        zarr_group = zarr.group(store, overwrite=True)
        chunk_size = np.array(em.chunk_size)[::-1]
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
                chunks=np.array(seg_vol.chunk_size[::-1]), 
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
                    em_cutout = em[x_chunk[0]:x_chunk[1], y_chunk[0]:y_chunk[1], z_chunk[0]:z_chunk[1]]
                    em_cutout = np.squeeze(em_cutout).transpose()
                    em_array[z_chunk[0]-z_start:z_chunk[1]-z_start, y_chunk[0]-y_start:y_chunk[1]-y_start, x_chunk[0]-x_start:x_chunk[1]-x_start] = em_cutout

                    if seg:
                        seg_cutout = seg_vol[x_chunk[0]:x_chunk[1], y_chunk[0]:y_chunk[1], z_chunk[0]:z_chunk[1]]
                        seg_cutout = np.squeeze(seg_cutout).transpose()
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