import os
import argparse
import fileinput

def mergeFile():
    parser = argparse.ArgumentParser()
    parser.add_argument("--datadir", help="direction stores data", type=str)
    parser.add_argument("--start-user", help="starting folder to merge", type=str)
    parser.add_argument("--end-user", help="ending folder to merge", type=str)
    parser.add_argument("--output", help="data output path", type=str)
    parser.add_argument("--container", help="number of users in each combine file")
    
    args = parser.parse_args()
    datadir = args.datadir
    start_folder = args.start_user
    end_folder = args.end_user
    output = args.output
    contain_num = args.container

    start_path_trajectory = datadir + '/' + start_folder + '/Trajectory/'
    end_path_trajectory = datadir + '/' + end_folder + '/Trajectory/'
    
    start_folder = int(start_folder)
    end_folder = int(end_folder)
    contain_num = int(contain_num)
    step = 1 if contain_num == 0 else contain_num

    for start_range_user in range(start_folder, end_folder + 1, step):
        end_range_user = start_range_user + (contain_num - 1)
        mergeLocationData(start_range_user, end_range_user, datadir, output)
        # mergeLabelDataFile(start_range_user, end_range_user, datadir, output)

def mergeLocationData(start_range_user, end_range_user, datadir, output):
    combine_trajectory = output + '/' + str(start_range_user).zfill(3) + '-' + str(end_range_user).zfill(3)
    with open(combine_trajectory + '.plt', 'w') as w:
            for folder in range(start_range_user, end_range_user + 1):
                folder_num = str(folder).zfill(3)
                full_data_path = datadir + '/' + folder_num + '/Trajectory/'
                list_dir_start = os.listdir(full_data_path)

                for _file in list_dir_start:
                    full_file_path = full_data_path + '/' + _file
                    fr = fileinput.input(files = (full_file_path,))
                    for line in fr:
                        # Before the first line has been read, returns 0
                        # skip 6 useless rows
                        if fr.lineno() > 6:
                            file_name = _file.split(".")[0]
                            w.write(folder_num + "," + file_name + "," + line)
                    w.write("end\n")
                    fr.close()
    w.close()

def mergeLabelDataFile(start_range_user, end_range_user, datadir, output):
    combine_labels = output + '/labels-' + str(start_range_user).zfill(3) + '-' + str(end_range_user).zfill(3) + '.txt'
    with open(combine_labels ,'w') as fw:
        for folder in range(start_range_user, end_range_user + 1):
            file_full_path = datadir + '/' + str(folder).zfill(3) + '/labels.txt'
            if (not os.path.isfile(file_full_path)):
                continue
            fr = fileinput.input(files=(file_full_path,))
            for line in fr:
                if fr.lineno() > 1:
                    line = line.replace("\t", ",")
                    fw.write(str(folder).zfill(3) + ',' + line)
            fw.write("end\n")
            fr.close()
        fw.close()

if __name__ == "__main__":
    mergeFile()


