import os
from decimal import Decimal

# dataPath = "/Users/richardlevinson/DshieldDemoData2022_Run1/"  # first  24 hours (1/4/20)
dataPath = "/Users/richardlevinson/DshieldDemoData2022_Run2/"    # second 24 hours (1/5/20)

def readMeasurementErrorTables():
    errTable1  = readMeasurementErrorTable(1)
    errTable2  = readMeasurementErrorTable(2)
    errTable3  = readMeasurementErrorTable(3)
    errTable4  = readMeasurementErrorTable(4)
    errTable5  = readMeasurementErrorTable(5)
    return {1 : errTable1, 2: errTable2, 3: errTable3, 4: errTable4, 5: errTable5}

def readMeasurementErrorTable(biomeId):
    errorTable = {}
    filepathIn = dataPath + "obs_quality/table_B"+str(biomeId)+".csv"
    print("\nReading errorTable file: " + filepathIn)
    if not os.path.exists(filepathIn):
        print("\nreadMeasurementErrorTable() ERROR! File not found: " + filepathIn + "\n")
        return
    isFirstLine = True
    fileIn = open(filepathIn, "r")
    for line in fileIn:
        line = line.strip()
        if not line.startswith("#"):
            if isFirstLine:
                isFirstLine = False
            else:
                terms = line.split(",")
                # print("terms: "+str(terms))
                column1 = int(terms[0])
                column2 = int(terms[1])
                if not (column1 == 0 and column2 == 0):
                    error = float(terms[2])
                    key = (column1, column2)
                    errorTable[key] = error
                # print("errorTable[" + str(key) + "] = " + str(error))
    fileIn.close()
    return errorTable

def getCmdErr(cmd, errTable):
    rowIndex = None
    choiceInfo = parseChoice(cmd)
    payload1 = choiceInfo['payload']
    angle1   = choiceInfo['pointingOption']
    code1 = getErrorTableCode(angle1)
    payload2 = choiceInfo['payload2'] if 'payload2' in choiceInfo else None
    angle2 = choiceInfo['pointingOption2'] if 'pointingOption2' in choiceInfo else None
    if not payload2:
        if payload1 == 'L':
            rowIndex = (code1,0)
        else:
            rowIndex = (0, code1)
    else:
        code2 = getErrorTableCode(angle2) # assumes payload1 is L, payload2 is P
        rowIndex = (code1, code2)
    return getError(rowIndex, errTable)



def getError(rowIndex, errorTable):
    if rowIndex in errorTable:
        return errorTable[rowIndex]
    else:
        print("getError() Error!  errorTable row not found for row index: "+str(rowIndex))
        return None

def getNormalizedScore(error):
    maxErr = Decimal("0.045")  # self.maxError = 0.032
    err = Decimal(str(error))
    normErr = err/maxErr
    normErrRound = round(normErr, 3)
    diff = Decimal("1.0") - normErrRound
    normErrFloat = round(float(diff), 3)
    return normErrFloat

def getErrorTableCode(pointingOption):
    # returns code for given pointingOption (code for 1 obs)
    if 28 <= pointingOption and pointingOption <= 35:
        return 1
    elif (22 <= pointingOption and pointingOption <= 27) or (36 <= pointingOption and pointingOption <= 41):
        return 2
    elif (14 <= pointingOption and pointingOption <= 21) or (42 <= pointingOption and pointingOption <= 49):
        return 3
    else:
        return 0

def getExtendedErrCode(codePair):
    codes = sorted(codePair)
    code1 = codes[0]
    code2 = codes[1]
    if code1 == code2:
        return code1 + 3
    elif code1 == code2:
        return code1 + 3
    elif code1 == 1:
        if code2 == 2:
            return 7
        elif code2 == 3:
            return 8
    elif code1 == 2 and code2 == 3:
        return 9
    print("getExtendedErrorCode() invalid code pair: "+str(codePair))

def getErrorTableTypeFromBiomeType(gpType):
    # coerced subtypes into errorTable types
    # if isinstance(gpType, str):
    #     gpType = biomeTypeFromLabel(gpType)
    #TODO: this mapping should be read from common/common.json
    if gpType in [1, 2, 3, 4, 5]:
        return 1  # forest
    elif gpType in [6, 7]:
        return 2 # shrubland
    elif gpType in [8, 9, 10]:
        return 3 # savanna
    elif gpType in [12, 14]:
        return 4 # cropland
    elif gpType == 16:
        return 5
    else:
        print("getErrorTableTypeFromBiomeType() ERROR gpType not found: "+str(gpType))
        return 5 # default

def biomeTypeFromLabel(biomeLabel):
    # NOTE: DSHIELD ignores these GP types: 17=water, 11=wetlands, 13=urban, 15=frozen
    biomeLabel = biomeLabel.lower()
    if biomeLabel == "Evergreen Needleleaf Forest".lower():
        return 1
    elif biomeLabel == "Evergreen Broadleaf Forest".lower():
        return 2
    elif biomeLabel == "Deciduous Needleleaf Forest".lower():
        return 3
    elif biomeLabel == "Deciduous Broadleaf Forest".lower() or biomeLabel == "Deciduous Broadleaf Forrest".lower():
        return 4
    elif biomeLabel == "Mixed Forests".lower():
        return 5
    elif biomeLabel == "Closed Shrublands".lower():
        return 6
    elif biomeLabel == "Open Shrublands".lower():
        return 7
    elif biomeLabel == "Woody Savannas".lower():
        return 8
    elif biomeLabel == "Savannas".lower():
        return 9
    elif biomeLabel == "Grasslands".lower():
        return 10
    elif biomeLabel == "Wetlands".lower(): # ignored by DSHIELD
        return 11
    elif biomeLabel == "Croplands".lower():
        return 12
    elif biomeLabel == "Urban".lower(): # ignored by DSHIELD
        return 13
    elif biomeLabel == "Cropland and Natural Mosaic".lower():
        return 14
    elif biomeLabel == "Frozen".lower():  # ignored by DSHIELD
        return 15
    elif biomeLabel == "Bare".lower():
        return 16
    elif biomeLabel == "Water".lower():  # ignored by DSHIELD
        return 17
    else:
        print("biomeTypeFromLabel() ERROR! unknown label: "+biomeLabel)
        return 7

def biomeLabel(type):
    # NOTE: DSHIELD ignores these GP types: 17=water, 11=wetlands, 13=urban, 15=frozen
    if type == 0:
        return "None"
    elif type == 1:
        return "Evergreen Needleleaf Forest"
    elif type == 2:
        return "Evergreen Broadleaf Forest"
    elif type == 3:
        return "Deciduous Needleleaf Forest"
    elif type == 4:
        return "Deciduous Broadleaf Forest"
    elif type == 5:
        return "Mixed Forests"
    elif type == 6:
        return "Closed Shrublands"
    elif type == 7:
        return "Open Shrublands"
    elif type == 8:
        return "Woody Savannas"
    elif type == 9:
        return "Savannas"
    elif type == 10:
        return "Grasslands"
    elif type == 11: # ignored by DSHIELD
        return "Wetlands"
    elif type == 12:
        return "Croplands"
    elif type == 13: # ignored by DSHIELD
        return "Urban"
    elif type == 14:
        return "Cropland and Natural Mosaic"
    elif type == 15:  # ignored by DSHIELD
        return "Frozen"
    elif type == 16:
        return "Bare"
    elif type == 17:  # ignored by DSHIELD
        return "Water"
    else:
        return type



def parseChoice(choice):
    result = {}
    choice = choice.strip("'")
    dotCount = choice.count(".")
    if dotCount == 1:
        payload, pointingOption = choice.split(".")
        pointingOption = int(pointingOption)
        errorTableCode = getErrorTableCode(pointingOption)
        result.update({"payload": payload, "pointingOption": pointingOption, "errorCode": errorTableCode})
    elif dotCount == 3:
        p1, a1, p2, a2 = choice.split(".")
        a1 = int(a1)
        a2 = int(a2)
        c1 = getErrorTableCode(a1)
        c2 = getErrorTableCode(a2)
        result.update({"payload": p1, "pointingOption": a1, "errorCode": c1, "payload2": p2, "pointingOption2": a2, "errorCode2": c2})
    return result

def convertDateTimeToFilenameFormat(inputFileDate, horizonId):
    month = str(inputFileDate.month).rjust(2, "0")
    date = str(inputFileDate.day).rjust(2, "0")
    hour = convertHorizonIdToHour(horizonId)
    result = str(inputFileDate.year)+month+date+"T"+hour+"00Z.csv"
    return result

def convertModelTimeToFilenameFormat(inputFileDate, hour):
    month = str(inputFileDate.month).rjust(2, "0")
    date = str(inputFileDate.day).rjust(2, "0")
    result = str(inputFileDate.year)+month+date+"T"+hour+"00Z.csv"
    return result

def convertHorizonIdToHour(horizonId):
    if horizonId == 1:
        return "0130"
    elif horizonId == 2:
        return "0730"
    elif horizonId == 3:
        return "1330"
    elif horizonId == 4:
        return "1930"
    else:
        print("convertHorizonIdToHour() ERROR! invalid horizonID: "+str(horizonId))

def getPrepPathForSat(satId):
    prepPath = dataPath+"planner/preprocessing/sat"+str(satId)+"/"
    return prepPath

def getPriorObservationFilename(filepath, fileprefix):
    files = os.listdir(filepath)
    for f in files:
        if f.startswith(fileprefix) and f.endswith(".plan.txt"):
            return f

def roundIt(n, precision=5):
    return round(n, precision)

def getUnassignedVarChoices(node, varName):
    vars = node.unassignedVars
    for var in vars:
        if var.name == varName:
            return list(var.choices.keys())

