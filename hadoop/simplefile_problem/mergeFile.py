import os


def mergeToFile(filename):
    with open("data/taxi_log_2008_by_id/" + filename,"w") as fw:
        all_files = os.listdir("data/taxi_log_2008_by_id")
        for _file in all_files:
        with open("data/taxi_log_2008_by_id/"+_file, "r") as fr:
            data = fr.read()
        fw.write(data)
        fr.close()
        fw.close()

if __name__ == "__main__":
    mergeToFile("combine.txt")