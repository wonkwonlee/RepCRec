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
    tm = TransactionManager.TransactionManager()

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

                # print("newLine :: " + newLine)
                
                temp = newLine.strip().strip(')')
                temp = temp.split('(')
                method = temp[0]
                
                # args = {}
                # if(len(args) > 0) :
                    # args = temp[1].split(',')
                # print("args :: {}".format(args))

                if method.startswith('begin'):
                    parameter1 = temp[1]
                    print(method, parameter1)
                    tm.begin(parameter1)

                elif method.startswith('beginRO'):
                    parameter1 = temp[1]
                    print(method, parameter1)

                elif method.startswith('W'):
                    args = temp[1].split(',')
                    parameter1 = args[0]
                    parameter2 = args[1]
                    parameter3 = args[2]
                    print(method, parameter1, parameter2, parameter3)

                elif method.startswith('R'):
                    args = temp[1].split(',')
                    parameter1 = args[0]
                    parameter2 = args[1]
                    print(method, parameter1, parameter2)

                elif method.startswith('fail'):
                    parameter1 = temp[1][0]
                    print(method, parameter1)

                elif method.startswith('recover'):
                    parameter1 = temp[1][0]
                    print(method, parameter1)

                elif method.startswith('end'):
                    parameter1 = temp[1][0]
                    print(method, parameter1)

                elif method.startswith('dump'):
                    print(method)
                else :
                    print("Unrecognized Command. Abort The Program")
                    break

                


        except IOError:
            print("CAN'T OPEN FILE {}".format(inputSource))

