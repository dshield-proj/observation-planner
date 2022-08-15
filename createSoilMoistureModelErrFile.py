import os
import random
import datetime
import time
import dshieldUtil

class SmmConverter:
    def __init__(self):
        # self.dataPath = "/Users/richardlevinson/DshieldDemoData2022_Run1/"  # first  24 hours (1/4/20)
        self.dataPath = "/Users/richardlevinson/DshieldDemoData2022_Run3/"    # second 24 hours (1/5/20)
        self.inputFileDate = datetime.date(2020,1,6) # y,m,d
        self.experimentRun = "RUN003" #"RUN002"  # "RUN001"
        self.allGp ={}
        self.soilMoistureModel = []
        self.allGpi = set()
        self.allModelGpi = set()
        self.stats = {}

    def convertModel(self):
        self.initStats()
        print("Preprocessing soil moisture model error data")
        self.initializeGpList() # TODO pass gplist in from dshieldObsPreprocessor
        self.readSoilMoistureFiles()
        both = self.allGpi.intersection(self.allModelGpi)
        modelGpCount = 0
        for gpi in self.allGp.keys():
            gpInfo = self.allGp[gpi]
            if 'modelErr' in gpInfo:
                modelGpCount += 1
        print("allGpi: "+str(len(self.allGpi))+", all model gpi: "+str(len(self.allModelGpi))+", intersect: "+str(len(both))+", modelGp count: "+str(modelGpCount))
        self.writeModelErrFile()
        self.printStats()

    def initializeGpList(self):
        filepath = self.dataPath + "common/grid.csv"
        print("\nReading GP data: "+filepath)
        if not os.path.exists(filepath):
            print("\ninitializeGpList() ERROR! File not found: " + filepath + "\n")
            return
        lineCount = 0
        with open(filepath, "r") as f:
            isFirstLine = True
            for line in f:
                if isFirstLine:
                    isFirstLine = False
                else:
                    line = line.strip()
                    terms = line.split(",")
                    gpi = int(terms[0])
                    type = int(terms[5])
                    biomeId = terms[6]
                    gpInfo = {"gp": gpi, "type": type, "biomeId": biomeId}
                    self.allGp[gpi] = gpInfo
                    self.allGpi.add(gpi)
                    lineCount += 1
                    if lineCount % 100000 == 0:
                        print(str(lineCount))
        # end with open file

        gpCount = len(self.allGp.keys())
        print("initializeGpList() GP count: "+str(gpCount))

    def readSoilMoistureFiles(self):
        modelTimes = [("0130", 0), ("0430", 3), ("0730",6), ("1030",9), ("1330",12), ("1630", 15), ("1930", 18), ("2230", 21)]
        for modelTime in modelTimes:
            filepathIn = self.dataPath + "target_value/" + self.experimentRun+"/targetVal_"
            fileSuffix = dshieldUtil.convertModelTimeToFilenameFormat(self.inputFileDate, modelTime[0])
            filepathIn += fileSuffix
            print("\nReading soilMoisture file: " + filepathIn)
            assert os.path.exists(filepathIn), "\nreadReducedErrorTable() ERROR! File not found: " + filepathIn + "\n"
            isFirstLine = True
            with open(filepathIn, "r") as f:
                for line in f:
                    if isFirstLine:
                        isFirstLine = False
                    else:
                        line = line.strip()
                        columns = line.split(",")
                        rowGp = int(columns[0])
                        rowErr = float(columns[1])
                        # realTime = self.convertTime(horizonId)
                        gp = self.getGP(rowGp)
                        gpType = gp["type"]
                        gpBiomeId = gp["biomeId"]
                        tick = modelTime[1]
                        row = {"time": tick, "gp": rowGp, "type": gpType, "biomeId": gpBiomeId, "err": rowErr}
                        self.soilMoistureModel.append(row)
                        self.allModelGpi.add(rowGp)
                        if rowGp in self.allGp.keys():
                            gp = self.allGp[rowGp]
                            if 'modelErr' not in gp:
                                gp.update({'modelErr': []})
                            pair = (tick, rowErr)
                            gp['modelErr'].append(pair)
                            # if gpType not in self.biomeBins:
                            #     self.biomeBins[gpType] = []
                            # self.biomeBins[gpType].append(gp)
                        else:
                            print("readSoilMoistureFiles() ERROR! gp not found: "+str(rowGp))
        print("soil moisture model ("+str(len(self.soilMoistureModel))+")")
        self.soilMoistureModel.sort(key=lambda row: (-row['err'], row['time']))
        hiErrCount = 0
        rowCount = 0
        for row in self.soilMoistureModel:
            rowCount += 1
            rowErr = row['err']
            if rowErr > 0.04:
                hiErrCount += 1
            # print(row)
        hiErrPct = round(hiErrCount/rowCount, 5)
        print("soil moisture summary: rowCount: "+str(rowCount)+", hiErrCount: "+str(hiErrCount)+", hiErr %: "+str(hiErrPct))


    def writeModelErrFile(self):
        prepPath = self.dataPath+"planner/preprocessing/"
        if not os.path.exists(prepPath):
            os.mkdir(prepPath)
        filepath = prepPath+"gpModelErr.txt"
        print("\n  Writing model error file: " + filepath)
        totalInitialErr = 0.0
        errCount = 0
        with open(filepath, "w") as f:
            for key in self.allGp:
                gpInfo = self.allGp[key]
                f.write(str(gpInfo)+"\n")
                errCount += 1
                initialErr = gpInfo['modelErr'][0]
                tick = initialErr[0]
                err = initialErr[1]
                if tick > 0:
                    print("ERROR! gp missing initial err: "+str(gpInfo))
                else:
                    totalInitialErr += err
        avgErr = round(totalInitialErr/errCount, 5)
        print("gpCount: "+str(len(self.allGp))+", errCount: "+str(errCount)+", avg initial err: "+str(avgErr))


    def convertTime(self, horizonId):
        # TODO: Should this be 1:30 for horizon 1 vs. 0?
        if horizonId == 1:
            return 0
        elif horizonId == 2:
            return 6
        elif horizonId == 3:
            return 12
        elif horizonId == 4:
            return 18
        else:
            print("convertTime() ERROR! invalid horizonId: "+horizonId)

    def getGP(self, index):
        if index not in self.allGp:
            print("getGP() ERROR! gp not found: "+str(index))
        else:
            return self.allGp[index]

    def initStats(self):
        self.stats["startTime"] = time.localtime()
        self.stats["startTimestamp"] = time.strftime("%H:%M:%S", self.stats["startTime"])
        self.stats["timerStart"] = time.time()

    def printStats(self):
        self.stats["timerEnd"] = time.time()
        self.stats["endTime"] = time.localtime()
        self.stats["endTimestamp"] = time.strftime("%H:%M:%S", self.stats["endTime"])
        elapsedMinutes, elapsedSecs = divmod(self.stats["timerEnd"] - self.stats["timerStart"], 60)
        elapsedHours, elapsedMinutes = divmod(elapsedMinutes, 60)
        elapsedString = ""
        if elapsedHours:
            elapsedString += str(elapsedHours)+ " h, "
        if elapsedMinutes:
            elapsedString += str(int(elapsedMinutes)) + " m, "
        elapsedString += format(elapsedSecs, '.3f')+" s"
        self.stats["elapsed"] = elapsedString
        print("\nTime: "+self.stats["startTimestamp"]+"-"+self.stats["endTimestamp"]+", elapsed: "+elapsedString)


if __name__ == '__main__':
    converter = SmmConverter()
    converter.convertModel()



