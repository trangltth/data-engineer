import jpype
import jpype.imports
from jpype import JClass, JObject
from jpype.types import *
import os

jpype.addClassPath('jars/*')
jpype.startJVM(jpype.getDefaultJVMPath(), convertStrings=False)

from org.apache.hadoop.conf import Configuration
from org.apache.hadoop.io.SequenceFile import Writer
from org.apache.hadoop.io import SequenceFile
from org.apache.hadoop.fs import Path
from org.apache.hadoop.io import BytesWritable
from org.apache.hadoop.io import Text

jwriter = jpype.JPackage('org.apache.hadoop.io.SequenceFile')

# store data in each files as key, value -> dict
# using SequenceFile to create sequenceFile
# upload to hadoop
def getAllFile(filesystempath):
    path = "data/input/taxi_log_2008_by_id"
    files_info = dict()
    all_files = os.listdir(path)
    for _file in all_files:
        with open(path + '/' + _file, 'rb') as _fr:
            content = _fr.read()
            key = path + '/' + _file
            files_info[key] = content
        _fr.close()
    
    transformToSequenceFile(filesystempath, files_info)

def transformToSequenceFile(filesystempath, files_mapping):
    conf =  Configuration()
    filesystem = Path(filesystempath)
    # writer = SequenceFile.createWriter(conf, Writer.keyClass(Text), Writer.valueClass(BytesWritable), filesystem)
    # for file in files_mapping.keys():
    #     writer.append(file, files_mapping[file])
    # writer.close()

if __name__ == "__main__":
    getAllFile("/user/trang/output/combine.seq")