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
                
                newLine = line.split('//')[0].strip()
                if len(newLine) == 0 :
                    continue
                elif newLine.startswith('quit') :
                    break

                print(newLine)
                
                temp = newLine.strip().strip(')')
                temp = temp.split('(')
                method = temp[0]
                # args = temp[1].split(',')
                # print("method :: {}".format(method))
                # print()

                if method.startswith('begin'):
                    print("begin")
                elif method.startswith('beginRO'):
                    print("beginRO")
                elif method.startswith('W'):
                    print("W")
                elif method.startswith('R'):
                    print("R")
                elif method.startswith('fail'):
                    print("fail")
                elif method.startswith('recover'):
                    print("recover")
                elif method.startswith('end'):
                    print("end")
                elif method.startswith('dump'):
                    print("dump")
                else :
                    print("Unrecognized Command. Abort The Program")
                    break

                


        except IOError:
            print("CAN'T OPEN FILE {}".format(inputSource))

