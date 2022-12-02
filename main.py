"""
Created on Friday, 2022-12-02

Author: Wonkwon Lee, Young Il Kim

"""

import sys
import TransactionManager

ts = 0

if __name__ == '__main__':
    """
    Read input from the command line and check for error
        : check number of inputs
        : check if the file exists
    """
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
            """
            Start Parsing and look for operation : begin, beginRO, W, R, fail, recover, end, dump
            Look for a deadlock before operation. 
            """
            for line in inputSource:
                if line == "":
                    continue
                
                newLine = line.split('//')[0].strip()
                if len(newLine) == 0 :
                    continue
                elif newLine.startswith('quit') :
                    break
                elif newLine.startswith('===') :
                    break
                
                temp = newLine.strip().strip(')')
                temp = temp.split('(')
                method = temp[0]
                
                # print("newLine :: " + newLine)
                # print("temp :: {}".format(temp))
                # print("method :: "+method)
                
                # args = {}
                # if(len(args) > 0) :
                    # args = temp[1].split(',')
                # print("args :: {}".format(args))

                ts += 1
                
                if tm.detect_deadlock2():
                     print("=========================== DEAD LOCK DETECHTED ===========================")
                     tm.run_operation()

                if method == "begin":
                    p1 = temp[1]
                    print(method, p1)
                    tm.begin(p1)

                elif method == "beginRO":
                    p1 = temp[1]
                    print(method, p1)
                    tm.beginRO(p1)

                elif method == "W":
                    args = temp[1].split(',')
                    p1 = args[0]
                    p2 = args[1]
                    p3 = args[2]
                    print(method, p1, p2, p3)
                    tm.write_operation(p1, p2, p3)


                elif method == "R":
                    args = temp[1].split(',')
                    p1 = args[0]
                    p2 = args[1]
                    print(method, p1, p2)
                    tm.read_operation(p1, p2)

                elif method == "fail":
                    p1 = temp[1]
                    print(method, p1)
                    tm.fail(p1)

                elif method == "recover":
                    p1 = temp[1]
                    print(method, p1)
                    tm.recover(p1)

                elif method == "end":
                    p1 = temp[1]
                    print(method, p1)
                    tm.end(p1)

                elif method == "dump":
                    print(method)
                    tm.dump()
                    
                else :
                    print("Unrecognized Command. Abort The Program")
                    break

                tm.run_operation()

        except IOError:
            print("CAN'T OPEN FILE {}".format(inputSource))
