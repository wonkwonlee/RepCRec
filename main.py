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

    # print("FILE NAME IS :: {}".format(fileName))
    # print()
    # print("INPUT SOURCE IS :: {}".format(inputSource))
    # print()

    if fileName :
        #print("INPUT :: {}".format(inputSource))
        try:
            #print("OPEN SUCCESSFULLY :: {}".format(inputSource))

            for line in inputSource:
                if line == "":
                    continue
                
                newLine = ''.join(filter(lambda c: c != ' ' and c != '\t' and c != '\n' and c is not None, line))
                newLine = line.split('//')[0].strip()
                print(newLine)


        except IOError:
            print("CAN'T OPEN FILE {}".format(inputSource))
