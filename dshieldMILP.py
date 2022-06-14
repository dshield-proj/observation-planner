import ast
import math
import random
import gc
from datetime import datetime
from os import path
from ortools.linear_solver import pywraplp
from gp import GP

class ObservationPlanner:

    def __init__(self):
        self.satList = [1]
        self.horizonId = 1
        self.horizonDur = 21600
        self.horizonFilter = "noRainOnly"  #"rainOnly" # rainOnly, noRainOnly, all
        self.timeoutMinutes = 30
        self.maxTick = 750
        self.horizonStart = ((self.horizonId - 1) * self.horizonDur) + 1 #21601 #1 #21601  #1
        self.horizonEnd = self.horizonStart + self.horizonDur - 1
        self.dataPath = "/Users/rlevinso/dshield/DshieldDemoData/"
        self.solver = pywraplp.Solver.CreateSolver('SCIP') #'CBC')

        self.horizonGPs = set()
        self.gpDict = {}
        self.satEvents = {}
        self.satEclipses = {}
        self.errorTable = {}
        self.slewTable = {}
        self.xVars = {}
        self.yVars = {}

        print("** Params **")
        print("satList: "+str(self.satList))
        print("timeoutMinutes: "+ str(self.timeoutMinutes))
        print("maxTick: "+str(self.maxTick))


    def createModel(self):
        self.createDecisionVars()
        self.createConstraints()
        self.createObjective2()
        lpModel = self.solver.ExportModelAsLpFormat(False)
        # model = self.solver.ExportModelAsMpsFormat(True, False)
        filename = "dshield."+self.horizonFilter+"."+str(self.maxTick)+"ticks.lp"
        filepath = self.dataPath + filename
        print("writing model to file: "+str(filepath))
        with open(filepath, "w") as f:
            f.write(str(lpModel))
        print("model file writing complete")

    def createDecisionVars(self):
        self.createXdecisionVars()
        self.createYdecisionVars()

    def createXdecisionVars(self):
        # x[i, t, c] = 1 iff sat i executes command c at time t
        solver = self.solver
        for satId in self.satList:
            vars = {}
            satEvents = self.satEvents[satId]
            for tick in satEvents.keys():
                event = satEvents[tick]
                choices = event.keys()
                for choice in choices:
                    # TODO: filter out choices with negative reward like y vars
                    varName = "x."+str(satId)+"."+str(tick)+"."+str(choice)
                    vars[tick, choice] = solver.BoolVar(varName)
            self.xVars[satId] = vars
        for satId in self.satList:
            print("createXdecisionVars() s"+str(satId)+": "+str(len(self.xVars[satId]))+" vars")

    def createYdecisionVars(self):
        # y[g,i, t, c] <= 1 if gps g is covered by sat i executing command c at time t
        # maximizing reward will find the single g with max reward
        solver = self.solver
        for satId in self.satList:
            vars = {}
            satEvents = self.satEvents[satId]
            for tick in satEvents.keys():
                event = satEvents[tick]
                choices = event.keys()
                for choice in choices:
                    gpList = event[choice]
                    for gpId in gpList:
                        varName = "y."+str(gpId)+"."+str(satId)+"."+str(tick)+"."+str(choice)
                        reward = self.getObjectiveReward(gpId, tick, choice)
                        if reward > 0:
                            var = solver.BoolVar(varName)
                            vars[gpId, tick, choice] = var
                            gp = self.getGP(gpId)
                            gp.yVars.append(var)
                        else:
                            print("ERROR! Negative Reward for "+varName+": "+reward)
            self.yVars[satId] = vars
        for satId in self.satList:
            print("createYdecisionVars() s"+str(satId)+": "+str(len(self.yVars[satId]))+" vars")

    def createConstraints(self):
        # pass
        self.createMutexConstraints()
        self.createGpCoverageConstraints()
        self.createDuplicateGpConstraints()

    def createMutexConstraints(self):
        for satId in self.satList:
            self.createMutexConstraintsForSat(satId)

    def createMutexConstraintsForSat(self, satId):
        startTime = datetime.now()
        startTimestamp = startTime.strftime("%m/%d/%Y %H:%M:%S")
        maxSlewTime = 22
        xVars = self.xVars[satId]
        xVarKeys = list(xVars.keys())
        mutexCount = 0
        v1Index = 0
        print("createMutexConstraintsForSat() s"+str(satId) + ", start: "+startTimestamp+" xVars: "+str(len(xVarKeys)))
        for var1 in xVarKeys:
            v1Index += 1
            v2Count = 0
            tick1 = int(var1[0])
            cmd1 = var1[-1]
            terms1 = cmd1.split(".")
            angle1 = int(terms1[-1])
            v2Keys = xVarKeys[v1Index:] # v2 list starts after v1 (v1Index already incremented for this loop)
            if v1Index % 1000 == 0:
                print("\nv1Index: "+str(v1Index)+", v2Index count: "+str(len(v2Keys)))
            for var2 in v2Keys:
                v2Count += 1
                if var1 != var2:
                    tick2 = int(var2[0])
                    if tick2 > tick1 + 2 + maxSlewTime:
                        # print("done checking for conflicts for: "+str(var1)+", var2: "+str(var2))
                        break
                    else:
                        cmd2 = var2[-1]
                        terms2 = cmd2.split(".")
                        angle2 = int(terms2[-1])
                        if tick1 <= tick2: # and angle1 != angle2:
                            if v1Index % 1000 == 0:
                                print("v1Index: "+str(v1Index)+": v1: "+str(var1)+", v2: "+str(var2))
                            [slewTime, slewEnergy] = self.getSlewTimeAndEnergy(angle1, angle2)
                            if tick2 - tick1 < 3 + slewTime:
                                v1 = xVars[var1]
                                v2 = xVars[var2]
                                consName = "c1."+str(v1)+"."+str(v2)
                                self.solver.Add(v1 + v2 <= 1, consName)
                                mutexCount += 1
                        else:
                            print("createMutexConstraintsForSat() ERROR! Tick 2 "+str(tick2)+ " < Tick 1 "+str(tick1))

        endTime = datetime.now()
        endTimestamp = endTime.strftime("%m/%d/%Y %H:%M:%S")
        elapsedTime = endTime - startTime
        print("createMutexConstraintsForSat() s"+str(satId) + ", start: "+startTimestamp+ ", end: "+endTimestamp+ ", elapsed: "+str(elapsedTime))

        print("mutexes ("+str(mutexCount)+"):")


    def createMutexConstraintsForSatOld(self, satId):
        startTime = datetime.now()
        startTimestamp = startTime.strftime("%m/%d/%Y %H:%M:%S")
        print("createMutexConstraintsForSat() s"+str(satId) + ", start: "+startTimestamp)
        maxSlewTime = 22
        vars = self.xVars[satId]
        mutexes = []
        duplicates = []
        v1Index = 0
        for var1 in vars.keys():
            v1Index += 1
            v2Count = 0
            tick1 = int(var1[0])
            cmd1 = var1[-1]
            terms1 = cmd1.split(".")
            angle1 = int(terms1[-1])
            v2Keys = list(vars.keys())[v1Index:] # v2 list starts after v1 (v1Index already incremented for this loop)
            for var2 in v2Keys:
                v2Count += 1
                if var1 != var2:
                    tick2 = int(var2[0])
                    if tick2 > tick1 + 2 + maxSlewTime:
                        # print("done checking for conflicts for: "+str(var1)+", var2: "+str(var2))
                        break
                    else:
                        cmd2 = var2[-1]
                        terms2 = cmd2.split(".")
                        angle2 = int(terms2[-1])
                        if tick1 <= tick2: # and angle1 != angle2:
                            if v1Index % 1000 == 0:
                                print("v1Index: "+str(v1Index)+": "+str(var1)+", "+str(var2))
                            [slewTime, slewEnergy] = self.getSlewTimeAndEnergy(angle1, angle2)
                            if tick2 - tick1 < 3 + slewTime:
                                if (var2, var1) not in mutexes:
                                    mutexes.append((var1, var2))
                                else:
                                    duplicates.append((var1, var2))
        endTime = datetime.now()
        endTimestamp = endTime.strftime("%m/%d/%Y %H:%M:%S")
        elapsedTime = endTime - startTime
        print("createMutexConstraintsForSat() s"+str(satId) + ", start: "+startTimestamp+ ", end: "+endTimestamp+ ", elapsed: "+str(elapsedTime))

        print("mutexes ("+str(len(mutexes))+"):")
        xVars = self.xVars[satId]
        for pair in mutexes:
            # self.printMutex(pair)
            var1 = xVars[pair[0]]
            var2 = xVars[pair[1]]
            consName = "c1."+str(var1)+"."+str(var2)
            self.solver.Add(var1 + var2 <= 1, consName)

        print("duplicates ("+str(len(duplicates))+"):")
        # for pair in mutexes:
        #     self.printMutex(pair)


    def printMutex(self, pair):
        print(str(pair[0])+ " + "+str(pair[1])+" <= 1")

    def createGpCoverageConstraints(self):
        # y_{g,i,c,t} <= x_{i,c,t}
        # constrain y by relating GP g to command c which covers it
        # y[g,i, t, c] <= 1 if gps g is covered by sat i executing command c at time t
        # maximizing reward will find the single g with max reward
        constraintCount = 0
        solver = self.solver
        for satId in self.satList:
            xVars = self.xVars[satId]
            yVars = self.yVars[satId]
            satEvents = self.satEvents[satId]
            for tick in satEvents.keys():
                event = satEvents[tick]
                choices = event.keys()
                for choice in choices:
                    xVarKey = (tick, choice)
                    xVar = xVars[xVarKey]
                    gpList = event[choice]
                    for gpId in gpList:
                        yVarKey = (gpId, tick, choice)
                        yVar = yVars[yVarKey]
                        consName = "c2."+str(yVar)+" LE "+str(xVar)
                        solver.Add(yVar <= xVar, consName)
                        constraintCount += 1
        print("createGpCoverageConstraints() created "+str(constraintCount) +" constraints")

    def createDuplicateGpConstraints(self):
        # no more than d_max # of duplicate observation of any GP
        # sum_{i,c,t} y_{g,i,c,t} <= d_max
        dMax = 1
        consCount = 0
        for g in self.horizonGPs:
            yVarsForG = self.getYvarsForGP(g)
            consName = "c3."+str(g)+".duplicates"
            self.solver.Add(self.solver.Sum(yVarsForG) <= dMax, consName)
            consCount += 1
        print("createDuplicateGpConstraints() created "+str(consCount)+" constraints")

    def getYvarsForGP(self,gpi):
        gp = self.getGP(gpi)
        return gp.yVars

    def getYvarsForGPOld(self,gp):
        result = []
        for satId in self.satList:
            yVarsForSat = self.yVars[satId]
            for key in yVarsForSat.keys():
                if key[0] == gp:
                    yVarForG = yVarsForSat[key]
                    result.append(yVarForG)
        return result

    def createObjective(self):
        objectiveTerms = []
        for satId in self.satList:
            yVars = self.yVars[satId]
            for key in yVars.keys():
                yVar = yVars[key]
                gp = key[0]
                cmd = key[1]
                tick = 0  # TODO: loop thru ticks
                reward = self.getObjectiveReward(gp, tick, cmd)
                objectiveTerms.append(reward * yVar)
        self.solver.Maximize(self.solver.Sum(objectiveTerms))

    def createObjective2(self):
        objectiveTerms = []
        count = 0
        for satId in self.satList:
            yVars = self.yVars[satId]
            for key in yVars.keys():
                yVar = yVars[key]
                gp =   key[0]
                tick = key[1]  # TODO: loop thru ticks
                cmd =  key[2]
                reward = self.getObjectiveReward(gp, tick, cmd)
                if reward > 0:
                    objectiveTerms.append((reward, yVar))
                    count += 1
                else:
                    print("ERROR! negative reward for "+str(key))
        objective = self.solver.Objective()
        for term in objectiveTerms:
            coef = term[0]
            var  = term[1]
            objective.SetCoefficient(var, coef)
        objective.SetMaximization()
        print("createObjective() terms: "+str(count))

    def getObjectiveReward(self, gpi, tick, cmd):
        gp = self.getGP(gpi)
        modelTime = self.getModelTime(tick)
        priorErr = self.getGpModelErr(gp, modelTime)
        errorTableType = self.getErrorTableTypeFromBiomeType(gp.type)
        if errorTableType not in self.errorTable:
            print("getObjectiveReward() ERROR! unknown biome type for gp: " + str(gpi))
            errorTableType = 7
        cmdErr = self.getCmdErr(cmd, self.errorTable[errorTableType])
        reward = priorErr - cmdErr
        return round(reward,4)

    def getCmdErr(self, cmd, errTable):
        rowIndex = None
        choiceInfo = self.parseChoice(cmd)
        payload1 = choiceInfo['payload']
        angle1   = choiceInfo['pointingOption']
        code1 = self.getErrorTableCode(angle1)
        payload2 = choiceInfo['payload2'] if 'payload2' in choiceInfo else None
        angle2 = choiceInfo['pointingOption2'] if 'pointingOption2' in choiceInfo else None
        if not payload2:
            if payload1 == 'L':
                rowIndex = (code1,0)
            else:
                rowIndex = (0, code1)
        else:
            code2 = self.getErrorTableCode(angle2) # assumes payload1 is L, payload2 is P
            rowIndex = (code1, code2)
        return self.getError(rowIndex, errTable)

    def getErrorTableCode(self, pointingOption):
        # returns code for given pointingOption (code for 1 obs)
        if 28 <= pointingOption and pointingOption <= 35:
            return 1
        elif (22 <= pointingOption and pointingOption <= 27) or (36 <= pointingOption and pointingOption <= 41):
            return 2
        elif (14 <= pointingOption and pointingOption <= 21) or (42 <= pointingOption and pointingOption <= 49):
            return 3
        else:
            return 0

    def getError(self, rowIndex, errorTable):
        if rowIndex in errorTable:
            return errorTable[rowIndex]
        else:
            print("getError() Error!  errorTable row not found for row index: "+str(rowIndex))
            return None

    def getModelTime(self, tick):
        ticksPerHour = 60*60
        if tick < 3 * ticksPerHour:
            return 0
        elif tick < 6 * ticksPerHour:
            return 3
        elif tick < 9 * ticksPerHour:
            return 6
        elif tick < 12 * ticksPerHour:
            return 9
        elif tick < 15 * ticksPerHour:
            return 12
        elif tick < 18* ticksPerHour:
            return 15
        elif tick < 21 * ticksPerHour:
            return 18
        else:
            return 21

    def parseChoice(self, choice):
        result = {}
        choice = choice.strip("'")
        dotCount = choice.count(".")
        if dotCount == 1:
            payload, pointingOption = choice.split(".")
            pointingOption = int(pointingOption)
            errorTableCode = self.getErrorTableCode(pointingOption)
            result.update({"payload": payload, "pointingOption": pointingOption, "errorCode": errorTableCode})
        elif dotCount == 3:
            p1, a1, p2, a2 = choice.split(".")
            a1 = int(a1)
            a2 = int(a2)
            c1 = self.getErrorTableCode(a1)
            c2 = self.getErrorTableCode(a2)
            result.update({"payload": p1, "pointingOption": a1, "errorCode": c1, "payload2": p2, "pointingOption2": a2, "errorCode2": c2})
        return result

    def solveIt(self):
        startTime = datetime.now()
        startTimestamp = startTime.strftime("%m/%d/%Y %H:%M:%S")
        # self.solver.InitGoogleLogging()
        self.solver.EnableOutput()
        print("solver time limit: "+str(self.timeoutMinutes)+" minutes")
        self.solver.set_time_limit(self.timeoutMinutes * 60 * 1000) # microsecs
        print("Solving...start time: "+str(startTimestamp))
        statusCode = self.solver.Solve()
        status = self.getStatus(statusCode)
        print("Done! Status: "+status+", value: "+str(self.solver.Objective().Value()))
        endTime = datetime.now()
        endTimestamp = endTime.strftime("%m/%d/%Y %H:%M:%S")
        self.elapsedTime = endTime - startTime
        elapsedTime = endTime - startTime
        print("\nSolver Done! Started: "+startTimestamp+", Ended: "+endTimestamp+", Elapsed: "+str(self.elapsedTime))
        self.printSolution()

    def loadPreprocessingResults(self):
        self.readSlewTable()
        self.errorTable = self.readAllReducedErrorTables()
        self.initializeEvents()
        self.initialHorizonGpErrAvg = self.getInitialHorizonGpErrAvg()

    def deletePreprocessingData(self):
        del self.horizonGPs
        del self.gpDict
        del self.satEclipses
        del self.errorTable
        del self.slewTable
        print("deletePreprocessingData() calling GC "+self.timestampNow())
        gc.collect()
        print("deletePreprocessingData() done "+self.timestampNow())

    def initializeEvents(self):
        for satId in self.satList:
            self.readHorizonGpFile(satId)
            self.readFlatHorizonFile(satId)
        self.filterNegativeRewards()

    def timestampNow(self):
        now = datetime.now()
        timestamp = now.strftime("%m/%d/%Y %H:%M:%S")
        return timestamp

    def filterNegativeRewards(self):
        removedTpCount = 0
        removedGpCount = 0
        removedCmdCount = 0
        # filter gp
        print("removing gp with negative rewards")
        for satId in self.satList:
            satEvents = self.satEvents[satId]
            satTimes = list(satEvents.keys())
            for tick in satTimes:
                event = satEvents[tick]
                # event: {'L.33': [195912, 195913, 195914, 195915], 'L.34': [195910, 195911], 'P.32': [195914, 195915]}
                cmdChoices = list(event.keys())
                for cmd in cmdChoices:
                    gpList = event[cmd]
                    filteredGpList = []
                    for gpId in gpList:
                        reward = self.getObjectiveReward(gpId, tick, cmd)
                        if reward > 0:
                            filteredGpList.append(gpId)
                        else:
                            # print("removing gp: "+str(gpId))
                            removedGpCount += 1
                    if filteredGpList:
                        event[cmd] = filteredGpList
                    else:
                        # print("removing cmd: "+str(cmd))
                        event.pop(cmd)
                        removedCmdCount += 1
                if event:
                    satEvents[tick] = event
                else:
                    # print("removing tp: "+str(cmd))
                    removedTpCount += 1
                    satEvents.pop(tick)

            self.satEvents[satId] = satEvents

        print("filterNegativeRewards() removed gps: "+str(removedGpCount)+", removed cmds: "+str(removedCmdCount)+", removed tps: "+str(removedTpCount))

    def readHorizonGpFile(self, satId):
        filename = self.getHorizonFilenamePrefix(satId, self.horizonId)+".gp.txt"
        filepath = self.dataPath + filename
        lineNumber = 0
        duplicateGP = 0
        print("readHorizonGpFile() file: "+str(filepath))
        if path.exists(filepath):
            lineCount = 0
            print("reading gp file: "+filename)
            with open(filepath, "r") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    pos = line.find(":")
                    line = line[pos+2:]
                    # print("ast line: "+str(line))
                    gpLine = ast.literal_eval(line)
                    gp = self.parseGpDict(gpLine)
                    newChoices = []
                    if gp.id in self.gpDict:
                        # print("readHorizonGpFile() ERROR! duplicate GP id: "+str(gp.id)+", new gp: "+str(gp)+", prior gp: "+str(self.gpDict[gp.id]))
                        duplicateGP += 1
                        oldErrorTable = self.gpDict[gp.id].errorTableChoices
                        newErrorTable = gp.errorTableChoices
                        for newChoice in newErrorTable:
                            isDuplicateChoice = False
                            for oldChoice in oldErrorTable:
                                if newChoice == oldChoice:
                                    # print("readHorizonGpFile() duplicate choice: "+str(newChoice))
                                    isDuplicateChoice = True
                            if not isDuplicateChoice:
                                newChoices.append(newChoice)
                        if newChoices:
                            # print("new choices: "+str(newChoices))
                            combinedErrorTable = []
                            combinedErrorTable.extend(oldErrorTable)
                            combinedErrorTable.extend(newChoices)
                            gp.errorTableChoices = combinedErrorTable
                    self.gpDict[gp.id] = gp
                    lineNumber += 1
                    if lineNumber % 10000 == 0:
                        print("  line: "+str(lineNumber))
            print("GP count: "+str(lineNumber) + ", duplicate GP count: "+str(duplicateGP))
        else:
            print("readHorizonGpFile() ERROR! file not found: "+filepath)

    def parseGpDict(self, dict):
        gp = GP(dict["gp"], None, None, "0")  # no lat, lon
        gp.type = dict["type"]
        if "rain" in dict:
            gp.rainHours = dict["rain"]
        if "accessTimes" in dict:
            gp.accessTimes = dict["accessTimes"]
        if "horizonAccessTimes" in dict:
            gp.horizonAccessTimes = dict["horizonAccessTimes"]
        if "filteredAccessTimes" in dict:
            gp.filteredAccessTimes = dict["filteredAccessTimes"]
        if "accessTimePairs" in dict:
            gp.accessTimePairs = dict["accessTimePairs"]
        if "pointingChoices" in dict:
            gp.pointingChoices = dict["pointingChoices"]
        if "errorChoices" in dict:
            gp.errorChoices = dict["errorChoices"]
        if "errorTableChoices" in dict:
            gp.errorTableChoices = dict["errorTableChoices"]
        if "initialModelError" in dict:
            gp.initialModelError = dict["initialModelError"]
        if "lat" in dict:
            gp.lat = dict["lat"]
        if "lon" in dict:
            gp.lon = dict["lon"]
        return gp

    def readFlatHorizonFile(self, satId):
        filename = self.getHorizonFilenamePrefix(satId, self.horizonId) + ".flat.txt"
        filepath = self.dataPath + filename
        print("\nreadFlatHorizonFile() Reading horizon file: " + filename)
        if not path.exists(filepath):
            print("\nreadFlatHorizonData() ERROR! File not found: " + filepath + "\n")
            return
        lastDict = {}
        dictLines = ""
        lineCount = 0
        horizonEvents = {}
        totalTpChoices = 0
        maxTpChoices = 0
        maxTpChoicesTp = None
        with open(filepath, "r") as f:
            for line in f:
                lineCount += 1
                line = line.strip()
                # print("line: "+str(line))
                if not line.startswith("#"):
                    if line.startswith("{"):
                        dictLines = line
                    else:
                        dictLines += line
                    if line.endswith("}"):
                        d = ast.literal_eval(dictLines)
                        tp = list(d.keys())[0]
                        if self.maxTick and tp > self.maxTick:
                            break
                        choices = d[tp]
                        # NOTE:  tick = tp - 1 if self.imageLock else tp
                        choiceCombos = self.getChoiceCombos(choices)
                        choiceCount = len(choiceCombos)
                        totalTpChoices += choiceCount
                        if choiceCount > maxTpChoices:
                            maxTpChoices = choiceCount
                            maxTpChoicesTp = {"tp": tp, "choices": choices, "combos": choiceCombos}
                        horizonEvents[tp] = choices
                        dictLines = ""
                        # collect all unique gp
                        choiceKeys = list(choices.keys())
                        for choice in choiceKeys:
                            gpList = choices[choice]
                            for gpi in gpList:
                                self.horizonGPs.add(gpi)
                                if gpi not in self.gpDict:
                                    print("readFlatHorizonFile() ERROR! unknown gpi: "+str(gpi))
                                    gp = GP(gpi, None, None, "0") # no lat, lon, type
                                    self.gpDict[gpi] = gp

                        lastDict = {tp: choices}
                        if lineCount % 1000 == 0:
                            print("tp: " + str(tp))

        gpDictSize = len(self.gpDict.keys())
        tpCount = len(horizonEvents.keys())
        print("\ns"+str(satId)+" tp count: " + str(tpCount) + ", gp count: " + str(len(self.horizonGPs)) +", gpDict count: "+str(gpDictSize)+ ", lineCount: " + str(lineCount))
        print("\ns"+str(satId)+" tp choices: " + str(totalTpChoices) + ", avgChoices/tp: " + str(totalTpChoices/tpCount)+ ", max choices/tp: "+str(maxTpChoices))
        maxChoiceTp = maxTpChoicesTp["tp"]
        maxChoiceChoices = maxTpChoicesTp["choices"]
        maxChoiceCombos = maxTpChoicesTp["combos"]

        print("lastDict: " + str(lastDict))
        self.satEvents[satId] = horizonEvents

    def readSlewTable(self):
        filename = self.dataPath + "slewTable.txt"
        print("Reading Slew Table")
        with open(filename, "r") as f:
            firstLine = True
            for line in f:
                if firstLine:
                    firstLine = False
                else:
                    terms = line.split(",")
                    poFrom = int(terms[0].strip())
                    poTo = int(terms[1].strip())
                    time = float(terms[2].strip())
                    energy = float(terms[3].strip())
                    if not poFrom in self.slewTable:
                        self.slewTable[poFrom] = {}
                    col = self.slewTable[poFrom]
                    col[poTo] = [time, energy]
        tableKeys = list(self.slewTable.keys())
        colCount = len(tableKeys)
        firstCol = self.slewTable[tableKeys[0]]
        rowCount = len(firstCol.keys())
        print("  slew table size: "+str(rowCount)+" x "+str(colCount))

    def getSlewTimeAndEnergy(self, fromAngle, toAngle):
        slewTableRow = self.slewTable[fromAngle]
        slewTime, slewEnergy = slewTableRow[toAngle]
        slewTimeCeil = math.ceil(slewTime)
        return (slewTimeCeil, slewEnergy)

    def getChoiceCombos(self, choices):
        singleCmds = []
        doubleCmds = []
        for cmd in choices.keys():
            terms = cmd.split(".")
            singleCmds.append(terms)
        for cmd1 in singleCmds:
            for cmd2 in singleCmds:
                if cmd1 != cmd2:
                    i1 = cmd1[0]
                    i2 = cmd2[0]
                    a1 = cmd1[1]
                    a2 = cmd2[1]
                    if i1 == 'L' and i2 == 'P' and a1 == a2:
                        dblCmd = cmd1 + cmd2
                        doubleCmds.append(dblCmd)
        combos = singleCmds + doubleCmds
        return combos

    def getHorizonFilenamePrefix(self, satId, hId, filter = None):
        hStart = ((hId - 1) * self.horizonDur) + 1 #21601 #1 #21601  #1
        hEnd = hStart + self.horizonDur - 1
        if not filter:
            filter = self.horizonFilter
        filename = "s" + str(satId) + "." + str(hStart) + "-" + str(hEnd) + "." + str(filter)
        return filename

    def getInitialHorizonGpErrAvg(self):
        totalErr = 0
        errCount = 0
        for gpi in self.horizonGPs:
            modelErr = self.getGpModelErr(gpi, 0)
            totalErr += modelErr
            errCount += 1
        return totalErr/errCount

    def getInitialObservedGpErrAvg(self):
        totalErr = 0
        for gpi in self.observedGPs:
            totalErr += self.getGpModelErr(gpi, 0)
        gpCount = len(self.observedGPs)
        avg = self.roundIt(totalErr/gpCount)
        result = {"totalErr": self.roundIt(totalErr), "gpCount": gpCount, "avgErr": avg}
        return result

    def getFinalObservedGpErrAvg(self):
        totalErr = 0
        for gpi in self.observedGPs:
            gp = self.getGP(gpi)
            totalErr += gp.finalModelError
        gpCount = len(self.observedGPs)
        avg = self.roundIt(totalErr/gpCount)
        result = {"totalErr": self.roundIt(totalErr), "gpCount": gpCount, "avgErr": avg}
        return result

    def getGpModelErr(self, gp, tick):
        if isinstance(gp, int):
            gp = self.getGP(gp)
        modelTime = self.getModelTime(tick) # convert 6 hour horizon into 3 hour time indices used by soil model
        modelErrors = gp.initialModelError
        for time, err in modelErrors:
            if time == modelTime:
                return err

    def getModelTime(self, tick):
        ticksPerHour = 60*60
        if tick < 3 * ticksPerHour:
            return 0
        elif tick < 6 * ticksPerHour:
            return 3
        elif tick < 9 * ticksPerHour:
            return 6
        elif tick < 12 * ticksPerHour:
            return 9
        elif tick < 15 * ticksPerHour:
            return 12
        elif tick < 18* ticksPerHour:
            return 15
        elif tick < 21 * ticksPerHour:
            return 18
        else:
            return 21

    def getGP(self, index):
        if index in self.gpDict:
            return self.gpDict[index]

    def readAllReducedErrorTables(self):
        errTable1  = self.readReducedErrorTable(1)
        errTable7  = self.readReducedErrorTable(7)
        errTable8  = self.readReducedErrorTable(8)
        errTable12 = self.readReducedErrorTable(12)
        errTable16 = self.readReducedErrorTable(16)
        return {1 : errTable1, 7: errTable7, 8: errTable8, 12: errTable12, 16: errTable16}

    def readReducedErrorTable(self, biomeType):
        errorTable = {}
        filepathIn = self.dataPath + "errorTable.igbp"+str(biomeType)+".txt"
        print("\nReading errorTable file: " + filepathIn)
        if not path.exists(filepathIn):
            print("\nreadReducedErrorTable() ERROR! File not found: " + filepathIn + "\n")
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
                    error = float(terms[2])
                    key = (column1, column2)
                    errorTable[key] = error
                    # print("errorTable[" + str(key) + "] = " + str(error))
        fileIn.close()
        return errorTable

    def roundIt(self, n, precision=5):
        return round(n, precision)

    def getStatus(self, status):
        if status == pywraplp.Solver.OPTIMAL:
            return "optimal"
        elif status == pywraplp.Solver.FEASIBLE:
            return "feasible"
        elif status == pywraplp.Solver.INFEASIBLE:
            return "infeasible"
        elif status == pywraplp.Solver.ABNORMAL:
            return "abnormal"
        elif status == pywraplp.Solver.UNBOUNDED:
            return "abnormal"
        elif status == pywraplp.Solver.NOT_SOLVED:
            return "notSolved"
        else:
            return "unknown"

    def getErrorTableTypeFromBiomeType(self, gpType):
        # coerced subtypes into errorTable types
        if isinstance(gpType, str):
            gpType = self.biomeTypeFromLabel(gpType)
        forestTypes = [1, 2, 4, 5]
        shrublandTypes = [6, 7]
        savannaTypes = [8, 9, 10]
        croplandTypes = [12, 14]
        bareTypes = [16]

        result = None

        if gpType in forestTypes:
            result = 1
        elif gpType in shrublandTypes:
            result = 7
        elif gpType in savannaTypes:
            result = 8
        elif gpType in croplandTypes:
            result = 12
        elif gpType in bareTypes:
            result = 16
        return result

    def biomeTypeFromLabel(self, biomeLabel):
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

    def printSolution(self):
        for satId in self.satList:
            print("\n*** Sat "+str(satId)+ " ***")
            plan = []
            observedGpList = []
            observedGpSet = set()
            rewardGpList = []
            satVars = self.xVars[satId]
            for xVarKey in satVars:
                var = satVars[xVarKey]
                if var.solution_value() > 0.0:
                    plan.append(xVarKey)
            print("\n Plan ("+str(len(plan))+")")
            for step in plan:
                print(str(step))
                gpList = self.getCoveredGp(satId, step[0], step[1])
                observedGpList.extend(gpList)
                observedGpSet.update(gpList)
            print("\n Observed GP: "+str(len(observedGpList)) +", unique: "+str(len(observedGpSet)))
            satVars = self.yVars[satId]
            for yVarKey in satVars:
                var = satVars[yVarKey]
                if var.solution_value() > 0.0:
                    gpi = yVarKey[0]
                    tick = yVarKey[1]
                    cmd = yVarKey[2]
                    reward = self.getObjectiveReward(gpi, tick, cmd)
                    result = (gpi, satId, cmd, tick, reward)
                    rewardGpList.append(result)
            totalReward = 0
            for reward in rewardGpList:
                totalReward += reward[4]
                print(str(reward))
            print("\nTotal reward ("+str(len(rewardGpList))+" GP): "+str(totalReward))


    def getCoveredGp(self, satId, tick, cmd):
        satEvents = self.satEvents[satId]
        event = satEvents[tick]
        choices = event.keys()
        for choice in choices:
            if choice == cmd:
                gpList = event[choice]
                return gpList

    def verifyPlan(self, filename):
        plan = []
        filepath = self.dataPath + filename
        with open(filepath, "r") as f:
            priorTick = None
            priorAngle = None
            for line in f:
                line = line.strip()
                if not line.startswith("#"):
                    parsedLine = ast.literal_eval(line)
                    tick = parsedLine[0]
                    cmd = parsedLine[1]
                    terms = cmd.split(".")
                    angle = int(terms[-1])
                    if priorTick and priorAngle:
                        tickDiff = tick - priorTick
                        [slewTime, slewEnergy] = self.getSlewTimeAndEnergy(priorAngle, angle)
                        if tick < priorTick + 2 + slewTime:
                            if slewTime > 0:
                                plan.append(('* Slew *', priorAngle, angle, slewTime, '**** ERROR! ****'))
                                print("ERROR! step: "+str(parsedLine)+" violates constraint with slew: "+str(slewTime))
                        elif slewTime > 0:
                            plan.append(('* Slew *', priorAngle, angle, slewTime))
                    plan.append(parsedLine)
                    priorTick = tick
                    priorAngle = angle

        filepath = self.dataPath + "planOut.txt"
        with open(filepath, "w") as f:
            for step in plan:
                f.write("\n"+str(step))
def main():
    planner = ObservationPlanner()
    planner.loadPreprocessingResults()
    # planner.verifyPlan("plan.s1.7h.in.txt")
    planner.createModel()
    # planner.deletePreprocessingData()
    # planner.solveIt()

if __name__ == '__main__':
    main()
