import TransactionManager
import sys
import os

if __name__ == '__main__':

    if len(sys.argv) != 2:
        print('INCORRECT INPUT')
        sys.exit(1)

    inputSource = sys.stdin
    fileName = open(sys.argv[1], 'r')
    if fileName.mode == 'r':
        inputSource = fileName.readlines()

    print("FILE NAME IS :: {}".format(fileName))
    print()
    print("INPUT SOURCE IS :: {}".format(inputSource))
    print()

    if fileName :
        print("INPUT :: {}".format(fileName))
        try:
            print("OPEN SUCCESSFULLY :: {}".format(fileName))
        except IOError:
            print("CAN'T OPEN FILE {}".format(fileName))
