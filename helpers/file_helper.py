# write down a file with current time stamp as iso format
import datetime
import os


def write_file(file_name, data):
    with open(file_name, "w") as f:
        f.write(data)
        f.close()


def read_file(file_name):
    with open(file_name, "r") as f:
        data = f.read()
        f.close()
    return data


def get_time():
    return datetime.datetime.now().isoformat()


def get_file_name(file_name):
    return os.path.join(os.getcwd(), file_name)


def write_time_stamp(file_name):
    write_file(get_file_name(file_name), get_time())


def read_time_stamp(file_name):
    content = read_file(get_file_name(file_name))
    if content:
        return content
    else:
        write_time_stamp(file_name)
        read_time_stamp(file_name)
        
