import sys, os
from java.io import FileOutputStream
sys.path.insert(1,'python_java/jars/hadoop-core-1.2.1.jar')
sys.path.insert(1,'python_java/jars/commons-logging-1.1.3.jar')
print(sys.path)
from org.apache.hadoop.conf import Configuration
from org.apache.hadoop.io.SequenceFile import Writer
# from org.apache.hadoop.io.SequenceFile.Writer import keyClass
# from org.apache.hadoop.io.SequenceFile.Writer import valueClass
from org.apache.hadoop.io import SequenceFile
from org.apache.hadoop.fs import Path
from org.apache.hadoop.io import BytesWritable
from org.apache.hadoop.io import Text
from org.apache.hadoop.fs import FileUtil
import java.io.File
import java.nio.file.Files

def getAllFile(filesystempath):
    path = "data/input/taxi_log_2008_by_id"
    files_info = dict()
    all_files = os.listdir(path)
    for _file in all_files:
        _fr = open(path + '/' + _file, 'rb')
        content = _fr.read()
        key = path + '/' + _file
        files_info[key] = content
        _fr.close()

    transformToSequenceFile(filesystempath, files_info)
		

def transformToSequenceFile(filesystempath, files_mapping):
    conf =  Configuration()
    filesystem = Path(filesystempath)
    writer = SequenceFile.createWriter(conf, Writer.keyClass(Text), Writer.valueClass(BytesWritable), filesystem)
    # for file in files_mapping.keys():
    #     writer.append(file, files_mapping[file])
    # writer.close()

if __name__ == "__main__":
    getAllFile("/user/trang/output/combine.seq")