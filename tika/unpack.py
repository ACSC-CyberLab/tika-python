#!/usr/bin/env python
# encoding: utf-8
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from .tika import parse1, callServer, ServerEndpoint
import tarfile
from io import BytesIO, TextIOWrapper
import csv
from sys import version_info

# Python 3 introduced .readable() to tarfile extracted files objects - this
# is required to wrap a TextIOWrapper around the object. However, wrapping
# with TextIOWrapper is only required for csv.reader() in Python 3, so the
# tarfile returned object can be used as is in earlier versions.
_text_wrapper = TextIOWrapper if version_info.major >= 3 else lambda x: x

# Python CSV reader cannot handle strings with null chars in them,
# but metadata CSVs may include nulls.
# To work around this, escape the strings ('\x00') before parsing them,
# and unescape them again after parsing.
if version_info.major >= 3:
    # Python3 expects (unicode) strs as input and output
    _csv_encode = lambda x: x.encode('unicode-escape').decode('ascii')
    _csv_decode = lambda x: x.encode('ascii').decode('unicode-escape')
else:
    # Python2 expects (byte) strs as input and output
    _csv_encode = lambda x: x.decode('latin-1').encode('unicode-escape')
    _csv_decode = lambda x: x.decode('unicode-escape').encode('latin-1')

# Code based on Python doc example, see bottom of the page at
#  https://docs.python.org/2.7/library/csv.html
def _wrapped_csv(csv_data, dialect=csv.excel, **kwargs):
    def _escape_strs(raw_data):
        for line in raw_data:
            yield _csv_encode(line)
    # Escape input strings to avoid null chars being passed to csv
    csv_reader = csv.reader(_escape_strs(csv_data),
                           dialect=dialect, **kwargs)
    for row in csv_reader:
        # Decode back to unescaped strings
        yield [_csv_decode(c) for c in row]


def from_file(filename, serverEndpoint=ServerEndpoint):
    '''
    Parse from file
    :param filename: file
    :param serverEndpoint: Tika server end point (optional)
    :return:
    '''
    tarOutput = parse1('unpack', filename, serverEndpoint,
                       responseMimeType='application/x-tar',
                       services={'meta': '/meta', 'text': '/tika',
                                 'all': '/rmeta/xml', 'unpack': '/unpack/all'},
                       rawResponse=True)
    return _parse(tarOutput)


def from_buffer(string, serverEndpoint=ServerEndpoint):
    '''
    Parse from buffered content
    :param string:  buffered content
    :param serverEndpoint: Tika server URL (Optional)
    :return: parsed content
    '''
    status, response = callServer('put', serverEndpoint, '/unpack/all', string,
                                  {'Accept': 'application/x-tar'}, False,
                                  rawResponse=True)

    return _parse((status, response))


def _parse(tarOutput):
    parsed = {}
    if not tarOutput:
        return parsed
    elif tarOutput[1] is None or tarOutput[1] == b"":
        return parsed

    tarFile = tarfile.open(fileobj=BytesIO(tarOutput[1]))

    # get the member names
    memberNames = list(tarFile.getnames())

    # extract the metadata
    metadata = {}
    if "__METADATA__" in memberNames:
        memberNames.remove("__METADATA__")

        metadataMember = tarFile.getmember("__METADATA__")
        if not metadataMember.issym() and metadataMember.isfile():
            metadataFile = _text_wrapper(tarFile.extractfile(metadataMember))
            metadataReader = _wrapped_csv(metadataFile)
            for metadataLine in metadataReader:
                # each metadata line comes as a key-value pair, with list values
                # returned as extra values in the line - convert single values
                # to non-list values to be consistent with parser metadata
                assert len(metadataLine) >= 2

                if len(metadataLine) > 2:
                    metadata[metadataLine[0]] = metadataLine[1:]
                else:
                    metadata[metadataLine[0]] = metadataLine[1]

    # get the content
    content = ""
    if "__TEXT__" in memberNames:
        memberNames.remove("__TEXT__")

        contentMember = tarFile.getmember("__TEXT__")
        if not contentMember.issym() and contentMember.isfile():
            if version_info.major >= 3:
                content = _text_wrapper(tarFile.extractfile(contentMember), encoding='utf8').read()
            else:
                content = tarFile.extractfile(contentMember).read().decode('utf8')

    # get the remaining files as attachments
    attachments = {}
    for attachment in memberNames:
        attachmentMember = tarFile.getmember(attachment)
        if not attachmentMember.issym() and attachmentMember.isfile():
            attachments[attachment] = tarFile.extractfile(attachmentMember).read()

    parsed["content"] = content
    parsed["metadata"] = metadata
    parsed["attachments"] = attachments

    return parsed
