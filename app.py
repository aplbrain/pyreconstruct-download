from flask import Flask, render_template, request, make_response
from intern.convenience import array
from intern.remote.boss import BossRemote
import zarr
import math
import tempfile
import shutil
import requests
import boto3
from botocore.exceptions import ClientError
import time
import logging

app = Flask(__name__)

@app.route("/")
def home():
    return render_template("home.html")

@app.route("/download")
def download():

    try:

        # Define subvolume bounds. End slices are excluded
        x_start = int(request.args.get('x-start'))
        x_end = int(request.args.get('x-end'))
        y_start = int(request.args.get('y-start'))
        y_end = int(request.args.get('y-end'))
        z_start = int(request.args.get('z-start'))
        z_end = int(request.args.get('z-end'))

        # TODO: add this to the web form
        mip = 0

        # Calculate extents for readability later
        x_extent = x_end - x_start
        y_extent = y_end - y_start
        z_extent = z_end - z_start

        # Set S3 paths to data
        image_path = request.args.get('image-path')
        em = ""
        channels = requests.get("https://api.metadata.bossdb.org/api/v2/channels").json()["data"]
        for channel in channels:
            if channel["attributes"]["ID"] == image_path:
                if channel["attributes"]["ChannelType"] == "Image":
                    em = array(image_path)
                    break
                else:
                    raise ValueError("This channel is not an image channel.")
        if not em:
            raise ValueError("This channel does not exist.")
        # TODO: want to implement col/exp/chan select as three dropdowns
        
        # Validate the bounds of the subvolume
        uri = image_path.split("://")[1].split("/")
        br = BossRemote({"protocol": "https", "host": "api.bossdb.io", "token": "public"})
        cf = br.get_experiment(uri[0], uri[1]).coord_frame
        cf = br.get_coordinate_frame(cf)

        if x_start < cf.x_start or x_end > cf.x_stop:
            return f"""
            <h1>Error:</h1>
            <p>x range {(x_start, x_end)} is out of bounds for valid x range {(cf.x_start, cf.x_stop)}</p>
            """, 500
        if y_start < cf.y_start or y_end > cf.y_stop:
            return f"""
            <h1>Error:</h1>
            <p>x range {(y_start, y_end)} is out of bounds for valid x range {(cf.y_start, cf.y_stop)}</p>
            """, 500
        if z_start < cf.z_start or z_end > cf.z_stop:
            return f"""
            <h1>Error:</h1>
            <p>x range {(z_start, z_end)} is out of bounds for valid x range {(cf.z_start, cf.z_stop)}</p>
            """, 500
        # TODO: add size limit for dataset

        seg = False
        if request.args.get('seg-path'):
            seg_path = request.args.get('seg-path')
            for channel in channels:
                if channel["attributes"]["ID"] == seg_path:
                    seg = True
                    seg_vol = array(seg_path)
                    break
            if not seg:
                raise ValueError("This channel does not exist.")

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

            # Upload the file
            s3_client = boto3.client('s3')
            try:
                now = int(time.time())
                collection, experiment, channel = image_path.split("/")[2:]
                output_filename = f"{collection}_{experiment}_{now}.zarr.zip"
                s3_client.upload_file(f"{tmpdirname}.zip", "pyreconstruct-download", output_filename)
                url = f"https://pyreconstruct-download.s3.amazonaws.com/{output_filename}"
                return f'''
                    <p>This link will be valid for 24 hours.</p>
                    <a href={url}>{url}</a>
                '''
            except ClientError as e:
                logging.error(e)
                return make_response("Error uploading to S3", 500)

    except Exception as e:
        return "<h1>Error:</h1>" + str(e), 500
