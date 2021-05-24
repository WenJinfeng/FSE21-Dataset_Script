from internetarchive import download
import sys
import os

def download_():
	download('stackexchange', files='stackoverflow.com-Posts.7z', verbose=True)

def unzip():
    files = os.listdir('stackexchange')
    for f in files:
        if '.7z' in f:
            print('Unzip ', f)
            os.system('7z e stackexchange/'+f+' -oarchive/'+f)
    print('finish')

def main():
    download_()
    unzip()


if __name__ == '__main__':
    main()