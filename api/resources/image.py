"""
Author: Geng-Yuan Jeng <jeengbou@gmail.com>
Insight 2020C DE Project "BINARY: Brain Image graNARY"
RESTful API app
"""

from io import BytesIO
import urllib.parse
import time
import zipfile
import logging
from flask import send_file
from flask_restful import Resource, reqparse
from models.image import ImageModel

class Image(Resource):
    """ Image resource """
    parser = reqparse.RequestParser()
    parser.add_argument('name',
                        type=str,
                        required=True,
                        help="This field can not be blank!"
                        )
    parser.add_argument('store_id',
                        type=int,
                        required=True,
                        help="Every images need a store id!"
                        )

    def get(self, name):
        """ API GET """
        logging.info('Resource: [Image] >>> Getting file %s', name)
        images = ImageModel.find_by_name(name)
        if images:
            return Image.download_files(images)
        return {"message": "image not found"}, 404

    @classmethod
    def download_files(cls, rows):
        """ download jpg files """
        mem_file = BytesIO()
        with zipfile.ZipFile(mem_file, 'w') as tmpf:
            for row in rows:
                for img in row.images:
                    img = urllib.parse.unquote(img)
                    logging.debug('Resource: [Image] >>> zipping file %s', img)
                    data = zipfile.ZipInfo(img)
                    data.date_time = time.localtime(time.time())[:6]
                    data.compress_type = zipfile.ZIP_DEFLATED
                    tmpf.write(filename=img, compress_type=zipfile.ZIP_DEFLATED)
        mem_file.seek(0)
        return send_file(mem_file, attachment_filename='{}.zip'.format(rows[0].name),
                         as_attachment=True, cache_timeout=600, conditional=True)

class ImageList(Resource):
    """ ImageList resource """
    def get(self):
        """ API GET """
        return {"records": [img.json() for img in ImageModel.query.all()]}
        ## return {"records": list(map(lambda x: x.json(), ImageModel.query.all()))}
