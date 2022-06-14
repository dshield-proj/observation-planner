import ast
import time
import os
from obsSat import Satellite
from gp import GP
from decimal import Decimal
import dshieldUtil
import datetime
import shutil
from createSoilMoistureModelErrFile import SmmConverter

class ObsPreprocessor:
    def __init__(self):
        self.dataPath = "/Users/richardlevinson/DshieldDemoData2022/"
        self.gpDict = {}
        self.latDict = {} # maps latitude into GPI for reading saturation data
        self.slewTable = {}
        self.errorTable = {}
        self.allSats = []
        self.saturatedGPs = []
        self.gpCount = 0
        self.horizonEvents = {} # keys are satIds {satId1: {horizonEvents}, satId2: {horizonEvents}}
                                # vals are map from TP to access times
        self.horizonGPs = []
        self.horizonGPIs = set()
        self.horizonGPwritten = set()
        self.choiceCounts = {}
        self.singleAccessGP = []
        self.removedPointingOptionCount = 0
        self.maxAccessGP = None
        self.maxErrChoiceGP = None
        self.totalErrChoiceCount = 0
        self.gpWithErrChoicesCount = 0
        self.gpWithSingleObsBest = 0
        self.gpWithSingleObsSecondBest = 0
        self.errorDiffThreshold = .0002
        self.unknownBiomeTypes = set()
        self.gpModelErrors = {}

        # preprocessor config params
        self.createModelErrorFile       = False # True if soil moisture predictor model changed (pre-processes all horizons at once)
        self.payloadAccessFilesChanged  = True # True only if payload access files change (or a new sat is added to satList)
        self.createGpFile               = True # When do we need this? Possibly only for follow-up obs?
        self.restrictPointingOptions    = True # NOTE: This must be True for including choice error/scores
        self.includeChoiceErrors        = False

        # planner config params (configures output for planner's input)
        self.inputFileDate = None
        self.horizonId     = None
        self.horizonStart  = None
        self.horizonEnd    = None
        self.horizonDur    = 21600
        self.rainHourStart = None
        self.rainHourEnd   = None

        self.imageLock = False  # true if each accessTime lasts 3 ticks
        self.maxFollowUpSeparationMinutes = 120
        self.stats = {}

    def start(self, satList, inputFileDate, horizonId):
        self.horizonId = horizonId  # 1 2 3 4
        self.inputFileDate = inputFileDate
        self.horizonStart = ((self.horizonId - 1) * self.horizonDur) #  #0 #21600
        self.horizonEnd = self.horizonStart + self.horizonDur - 1
        self.initStats()
        self.initFileFolders(satList)
        if self.createModelErrorFile:
            converter = SmmConverter()
            converter.convertModel()
            del converter

        self.errorTable = dshieldUtil.readMeasurementErrorTables()

        # read GP data (covGrid.csv)
        self.initializeGpList() # defines all gp <gpId, lat, lon, gpType>

        # read raw payload access times (all horizons) if the raw inputs changed
        if self.payloadAccessFilesChanged:
            self.copyEclipseFilesToPreprocessingFolders(satList)
            for satId in satList:
                # read sat events for all horizons
                self.readPayloadEvents(satId) # populates accessTimes for self.gpDict[gpi]['accessTimes']
                self.collectSatEvents(satId)
                # write sat events for all horizons
                self.writeSatEventsFile(satId)

        # extract events for horizon only
        self.horizonEvents.clear()
        for satId in satList:
            self.readSatHorizonEvents(satId) # populates self.horizonEvents and self.horizonGPIs


        # update gp dynamic properties
        self.setGpHorizonAccessTimes()
        self.printGpAccessSummary() # all gp in covGrid
        self.setAllGpChoices()

        # write horizon files
        for satId in satList:
            # self.writeHorizonFile(satId, False)
            self.writeHorizonFile(satId, True) # save out version with flattenedChoices
        if self.createGpFile:
            self.readGpModelErrFile()
            self.writeHorizonGpFile()

        unknownGpTypes = sorted(list(self.unknownBiomeTypes))
        if unknownGpTypes:
            print("\n** UNKNOWN BIOME TYPES (coerced to default type 7 = Open Shrublands) **")
            for type in unknownGpTypes:
                print(str(type)+" "+str(dshieldUtil.biomeLabel(type)))
            print("\n")
        if self.maxErrChoiceGP:
            avgGpErrChoiceCount = self.totalErrChoiceCount / self.gpWithErrChoicesCount
            print("GP with errChoices: "+str(self.gpWithErrChoicesCount) +", avg gp err choices: "+format(avgGpErrChoiceCount, '.3f'))
            print("GP with single obs best count: "+str(self.gpWithSingleObsBest))
            print("GP with single obs second best count: "+str(self.gpWithSingleObsSecondBest)+" (err diff < "+str(self.errorDiffThreshold) +")")
            print("Total GP with single obs 1st or 2nd: "+str(self.gpWithSingleObsBest + self.gpWithSingleObsSecondBest))
            print("\nMax Err Choice GP ("+str(len(self.maxErrChoiceGP.errorTableChoices))+")")
            print(self.maxErrChoiceGP.prettyPrint())
        self.printStats()

    def getGPfromLatLon(self, lat, lon):
        # print("getGPfromLatLon() lat: "+lat+ ", lon: "+lon)
        if lat in self.latDict:
            gpList = self.latDict[lat]
            gpListCount = len(gpList)
            # if gpListCount > 1:
            #     print("getGPfromLatLon() multiple gpi for lat: "+lat+", gp count: "+str(gpListCount))
            for gpi in gpList:
                gp = self.getGP(gpi)
                if gp.lat == lat and gp.lon == lon:
                    return gp.id
        else:
            print("getGPfromLatLon() ERROR! gp not found for lat: "+lat+", lon: "+lon)

    def initializeGpList(self):
        filepath = self.dataPath + "common/grid.csv"
        print("\nReading GP data: "+filepath)
        if not os.path.exists(filepath):
            print("\nreadStaticGPdata() ERROR! File not found: " + filepath + "\n")
            return
        lineCount = 0
        with open(filepath, "r") as f:
            isFirstLine = True
            for line in f:
                if isFirstLine:
                    isFirstLine = False
                else:
                    terms = line.split(",")
                    # extract gpId, lat, lon, gpType (ignore first column)
                    gpi = int(terms[0])
                    lat = terms[1]
                    lon = terms[2]
                    type = int(terms[5])
                    biomeId = terms[6].strip()
                    gp = GP(gpi, lat, lon, type, biomeId)
                    self.gpDict[gpi] = gp
                    gpiList = self.latDict[lat] if lat in self.latDict else []
                    gpiList.append(gpi)
                    self.latDict[lat] = gpiList
                    lineCount += 1
                    if lineCount % 100000 == 0:
                        print(str(lineCount))
        # end with open file

        self.gpCount = len(self.gpDict.keys())
        print("readStaticGPdata() GP count: "+str(self.gpCount))


    def readPayloadEvents(self, satId):
        sat = Satellite(satId)
        self.allSats.append(sat)
        self.readPayloadAccessFileNewFormat2022(sat, 1)
        self.readPayloadAccessFileNewFormat2022(sat, 2)

    def collectSatEvents(self, satId):
        # collect aggregate set of events from both payloads
        print("collectSatEvents() sat: "+str(satId))
        sat = self.getSat(satId)
        print("  collecting payload1 events")
        for event in sat.payload1.accessEvents:
            eventTime = event["time"]
            gpi = event["gpi"]
            p1opts = event["pointingOpts"] # [48]
            payloadId = 1
            if eventTime not in sat.accessTimes:
                eventInfo = {} # {1235138: {1: [49]}}
                eventInfo[gpi] = {payloadId: p1opts}
                sat.accessTimes[eventTime] = eventInfo
            else:
                accessTimes = sat.accessTimes[eventTime]
                if gpi in accessTimes:
                    eventInfo = accessTimes[gpi]
                    eventInfo[payloadId] = p1opts
                else:
                    accessTimes[gpi] = {payloadId: p1opts}

        print("  collecting payload2 events")
        for event in sat.payload2.accessEvents:
            eventTime = event["time"]
            gpi = event["gpi"]
            p2opts = event["pointingOpts"]
            payloadId = 2
            if not eventTime in sat.accessTimes:
                eventInfo = {}
                eventInfo[gpi] = {payloadId: p2opts}
                sat.accessTimes[eventTime] = eventInfo
            else:
                accessTimes = sat.accessTimes[eventTime]
                if gpi in accessTimes:
                    eventInfo = accessTimes[gpi]
                    eventInfo[payloadId] = p2opts
                else:
                    accessTimes[gpi] = {payloadId: p2opts}

    def writeSatEventsFile(self, satId):
        # writes sat events FOR ALL HORIZONS
        print("writeSatEventsFile() sat: "+str(satId))
        # events file format:
        # {46: {1201153: {1: [46], 2: [46, 47]}}}
        # {47: {1200315: {1: [46], 2: [46, 47]},
        #       1200316: {1: [46], 2: [45, 46]}}}
        sat = self.getSat(satId)
        sortedAccessTimes = sorted(sat.accessTimes.keys())

        priorEventTime = None
        totalGapTime = 0
        maxGap = 0
        gapCount = 0
        accessTimeCount = 0
        maxEventTime = 0
        filename = dshieldUtil.getPrepPathForSat(satId)
        filename += "s" + str(satId)+".events.txt"
        print("Writing sat events file: "+filename)
        with open(filename, "w") as f:
            for eventTime in sortedAccessTimes:
                event = sat.accessTimes[eventTime]
                timestamp = str(eventTime)
                accessTimeCount += 1
                if eventTime > maxEventTime:
                    maxEventTime = eventTime
                if priorEventTime and eventTime - priorEventTime > 1:
                    gapTime = eventTime - priorEventTime - 1
                    totalGapTime += gapTime
                    gapCount += 1
                    if gapTime > maxGap:
                        maxGap = gapTime
                    msg = "# ~~ "+str(gapTime)+" sec gap ("+str(totalGapTime)+" total) ~~\n"
                    f.write(msg)
                priorEventTime = eventTime
                pad = " ".rjust(len(timestamp)+4)
                msg1 = "{"+timestamp+": {"
                msg2 = ""
                gpiKeys = sorted(event.keys())
                firstGpiKey =  gpiKeys[0]
                for gpi in gpiKeys:
                    if gpi == firstGpiKey:
                        msg1 += str(gpi)+": "+str(event[gpi])
                    else:
                        msg2 += ",\n"+pad+str(gpi)+": "+str(event[gpi])
                msg2 += "}}\n"
                f.write(msg1)
                f.write(msg2)

            # write out summary data
            avgGap = totalGapTime/gapCount
            allGpKeys = list(self.gpDict.keys())
            f.write("\n# END")
            f.write("\n# Access time count: "+str(accessTimeCount) +", max event time: "+str(maxEventTime)+", GP count: "+str(len(allGpKeys)))
            f.write("\n# Total gap time: "+str(totalGapTime)+", maxGap: "+str(maxGap)+", avgGap: "+str(avgGap)+", gapCount: "+str(gapCount)+"\n")


    def readSatHorizonEvents(self, satId):
        print("readSatHorizonEvents() sat: "+str(satId))
        # populates self.horizonEvents and self.horizonGPIs
        count = 0
        start = self.horizonStart
        end = self.horizonEnd
        saturatedGP = set()
        horizonStarted = False
        horizonDone = False
        filepath = dshieldUtil.getPrepPathForSat(satId)
        filepath += "s" +str(satId)+ ".events.txt"
        print("\nReading horizon data for s"+str(satId)+": "+str(start)+" - "+str(end)+", file: " + filepath)
        if not os.path.exists(filepath):
            print("\nreadHorizonEvents() ERROR! File not found: " + filepath + "\n")
            return
        lineCount = 0
        satHorizonEvents = {}
        with open(filepath, "r") as f:
            print("  scanning to horizon start ")
            while not horizonDone:
                dictDone = False
                dictLines = ""
                while not dictDone:
                    for line in f:
                        lineCount += 1
                        line = line.strip()
                        if line.startswith("# END"):
                            dictDone = True
                            horizonDone = True
                            print("\nreadHorizonEvents() no more data")
                            break
                        elif not line.startswith("#") and len(line) > 0:
                            dictLines += line
                            if not line.endswith(","):
                                dictDone = True
                                d = ast.literal_eval(dictLines)
                                tp = list(d.keys())[0]
                                adjustedTP = tp-1 if self.imageLock else tp # start one tick before access time to lock image for 3 secs
                                if not horizonStarted and (lineCount % 20000 == 0):
                                    print("  line: "+str(lineCount)+", tp: "+str(tp))
                                if tp >= start:
                                    if not horizonStarted:
                                        print("    horizon started")
                                        horizonStarted = True
                                    if tp >= end:
                                        horizonDone = True
                                    else:
                                        gpMap = {}
                                        for gpKey in d[tp].keys():
                                            gp = self.getGP(gpKey)
                                            if self.restrictPointingOptions:
                                                pointingOpts = d[tp][gpKey]
                                                filteredPointingOpts = self.removeRestrictedPointingOpts(pointingOpts)
                                                if filteredPointingOpts:
                                                    gpMap[gpKey] = filteredPointingOpts
                                                else:
                                                    gp.filteredAccessTimes.append((adjustedTP, "badAngle"))
                                            else:
                                                gpMap[gpKey] = d[tp][gpKey]

                                        if len(gpMap.keys()) > 0:
                                            satHorizonEvents[adjustedTP] = gpMap
                                        if lineCount % 1000 == 0:
                                            print("  line: "+str(lineCount)+", tp: "+str(tp))
                                break
                    # EOF
        #end with open file

        self.horizonEvents[satId] = satHorizonEvents
        # print summary info
        print("readSatHorizonEvents() sat: "+str(satId)+", tp count: " + str(len(satHorizonEvents.keys())) + ", saturatedCount: " + str(len(saturatedGP)) + ", removedPointingOpts: " + str(self.removedPointingOptionCount))
        # print("lastDict: "+str(lastDict))


    def setGpHorizonAccessTimes(self):
        print("setGpHorizonAccessTimes()")
        i = 0
        for sat in self.allSats:
            satId = sat.id
            satHorizonEvents = self.horizonEvents[satId]
            tpKeys = satHorizonEvents.keys()
            print("setGpHorizonAccessTimes() sat "+str(satId)+" access time count: "+str(len(tpKeys)))
            for tp in tpKeys:
                for gpi in satHorizonEvents[tp]:
                    self.horizonGPIs.add(gpi)
                    gp = self.getGP(gpi)
                    if tp not in gp.horizonAccessTimes:
                        gp.horizonAccessTimes.append(tp) # sets horizonAccessTime
                        if gp not in self.horizonGPs:
                            self.horizonGPs.append(gp)
                if i % 500 == 0:
                    print("count: "+str(i)+", tp: "+str(tp))
                i += 1
            print("setGpHorizonAccessTimes() sat "+str(satId)+" horizonGPI count: " + str(len(self.horizonGPIs)) + ", horizonGP count: " + str(len(self.horizonGPs)))


    def removeRestrictedPointingOpts(self, pointingOpts):
        result = {}
        for payloadKey in pointingOpts:
            newOpts = []
            for opt in pointingOpts[payloadKey]:
                if 14 <= opt and opt <= 49:
                    newOpts.append(opt)
                else:
                    # print("removeRestrictedPointingOpts() invalid opt: "+str(opt))
                    self.removedPointingOptionCount += 1
            if newOpts:
                result[payloadKey] = newOpts
        return result

    def writeHorizonFile(self, satId, flattenChoices):
        start  = self.horizonStart
        end    = self.horizonEnd
        print("writeHorizonFile() s"+str(satId)+" "+str(start)+" - "+str(end)+" "+" flatten: "+str(flattenChoices))
        satHorizonEvents = self.horizonEvents[satId]
        payloadHeader = self.getSat(satId).payload1.header
        sortedEventTimes = sorted(satHorizonEvents.keys())
        priorEventTime = None
        totalGapTime = 0
        maxGap = 0
        gapCount = 0
        accessTimeCount = 0
        maxEventTime = 0
        self.horizonGPwritten.clear()
        filename = "s"+str(satId)+"."+str(start)+"-"+str(end)
        if flattenChoices:
            filename += ".flat"
        filename += ".txt"
        filepath = dshieldUtil.getPrepPathForSat(satId)
        filepath += filename
        print("Writing horizon data file: "+filename)
        with open(filepath, "w") as f:
            for line in payloadHeader:
                f.write("# "+line+"\n")
            f.write("\n")
            for eventTime in sortedEventTimes:
                event = satHorizonEvents[eventTime]
                timestamp = str(eventTime)
                accessTimeCount += 1
                if eventTime > maxEventTime:
                    maxEventTime = eventTime
                if priorEventTime and eventTime - priorEventTime > 1:
                    gapTime = eventTime - priorEventTime - 1
                    totalGapTime += gapTime
                    gapCount += 1
                    if gapTime > maxGap:
                        maxGap = gapTime
                    msg = "# ~~ "+str(gapTime)+" sec gap ~~~\n" #"("+str(totalGapTime)+" total, count: "+str(gapCount)+") ~~\n"
                    f.write(msg)
                priorEventTime = eventTime
                pad = " ".rjust(len(timestamp)+4)
                msg1 = "{"+timestamp+": {"
                msg2 = ""
                if flattenChoices:
                    tpChoices = self.flattenChoices(eventTime, event)
                    if self.includeChoiceErrors:
                        self.addChoiceScores(eventTime, tpChoices)
                    # tpChoices = self.flattenAllChoices(eventTime, event)
                    choiceKeys = sorted(tpChoices.keys())
                    firstKey = choiceKeys[0]
                    for key in choiceKeys:
                        choiceVal = tpChoices[key]
                        for gpi in choiceVal:
                            self.horizonGPwritten.add(gpi)
                        if key == firstKey:
                            # msg1 += "'"+str(key)+"': "+str(choiceVal)
                            msg1 += str(key)+": "+str(choiceVal)
                        else:
                            # msg2 += ",\n"+pad+"'"+str(key)+"': "+str(choiceVal)
                            msg2 += ",\n"+pad+str(key)+": "+str(choiceVal)
                else:
                    gpiKeys = sorted(event.keys())
                    firstGpiKey = gpiKeys[0]
                    for gpi in gpiKeys:
                        self.horizonGPwritten.add(gpi)
                        gpiEvent = event[gpi]
                        if gpi == firstGpiKey:
                            msg1 += str(gpi)+": "+str(gpiEvent)
                        else:
                            msg2 += ",\n"+pad+str(gpi)+": "+str(gpiEvent)
                msg2 += "}}\n"
                f.write(msg1)
                f.write(msg2)

            # write out summary data
            avgGap = totalGapTime/gapCount if gapCount > 0 else None
            gpCount = len(self.horizonGPIs)
            gp2Count = len(self.horizonGPwritten)
            f.write("\n# Access time count: "+str(accessTimeCount) +", max event time: "+str(maxEventTime)+", GP count: "+str(gpCount)+", GP2 count: "+str(gp2Count))
            f.write("\n# Total gap time: "+str(totalGapTime)+", maxGap: "+str(maxGap)+", avgGap: "+str(avgGap)+", gapCount: "+str(gapCount)+"\n")
        # end with open file

        print("\ntp count: " + str(accessTimeCount) + ", horizon gp count: " + str(gpCount) + ", gp written count: " + str(gp2Count))
        # TODO: why is it writing more gp (gp2) than gp1? Maybe only only after gap filling
        #     (are we writing duplicates?)
        if flattenChoices:
            tpCount = 0
            initialTotal = 0
            finalTotal = 0
            diffTotal = 0
            for tp in self.choiceCounts.keys():
                tpCount += 1
                counts = self.choiceCounts[tp]
                initialTotal += counts["initial"]
                finalTotal += counts["final"]
                diffTotal += counts["diff"]
            diffAvg = format(diffTotal / tpCount, '.3f')
            diffPercentage = format(finalTotal / initialTotal, '.3f')
            print("Flattened choices: initial: " + str(initialTotal) + ", final: " + str(finalTotal) + ", diff: " + str(
                diffTotal) + ", avgDiff: " + str(diffAvg) + ", " + str(finalTotal) + "/" + str(
                initialTotal) + " = " + str(diffPercentage) + " %")

    def setAllGpChoices(self):
        print("setAllGpChoices()")
        singleAccessCountTotal = len(self.singleAccessGP)
        singleAccessHorizonGP = []
        doubleAccessHorizonGP = []
        moreAccessHorizonGP = []
        followUpPairs = {}
        gpWithMultipleFollowUpCount = 0
        for gpi in self.horizonGPIs:
            gp = self.getGP(gpi)
            accessCount = len(gp.accessTimes)
            if accessCount == 1:
                singleAccessHorizonGP.append(gp)
            elif accessCount == 2:
                doubleAccessHorizonGP.append(gp)
            elif accessCount > 2:
                moreAccessHorizonGP.append(gp)
            followUpPair = None
            if self.restrictPointingOptions:
                followUps = self.collectFollowUpTpPairs(gp)
                if followUps:
                    followUpPairs[gpi] = followUps
                    gp.accessTimePairs = followUps

                    # set gp errorChoices
                    followUpPair = gp.accessTimePairs[0]        # NOTE: currently only considers the first pair
                    self.setGpChoicesForFollowUpPair(gp, followUpPair)
                    if len(followUps) > 1:
                        gpWithMultipleFollowUpCount += 1
            for tp in gp.horizonAccessTimes:
                if not gp.errorTableChoices or not followUpPair or tp not in followUpPair:
                    self.setAllGpChoicesForSingleAccessTime(gp, tp)

        singleAccessHorizonGPCount = len(singleAccessHorizonGP)
        doubleAccessHorizonGPCount = len(doubleAccessHorizonGP)
        moreAccessHorizonGPCount = len(moreAccessHorizonGP)
        gpWithFollowUpCount = len(followUpPairs.keys())

        print("\n  horizonGP: " + str(len(self.horizonGPIs)) + ", singleAccessHorizonGP: " + str(singleAccessHorizonGPCount) + ", total singleAccess: " + str(singleAccessCountTotal))
        print("  doubleAccessGP: "+str(doubleAccessHorizonGPCount)+", moreAccessHorizonGP: "+str(moreAccessHorizonGPCount))
        print("  gpWithFollowUpCount: "+str(gpWithFollowUpCount)+", gpWithMultipleFollowUpCount: "+str(gpWithMultipleFollowUpCount))

    def collectFollowUpTpPairs(self, gp):
        # TODO: handle multi-sat
        accessTimes = sorted(gp.horizonAccessTimes)
        result = []
        for t1 in accessTimes:
            if t1 > self.horizonEnd:
                break
            else:
                for t2 in accessTimes:
                    if t2 > self.horizonEnd:
                        break
                    elif t1 in self.horizonEvents and t2 in self.horizonEvents:
                        tpChoices1 = self.horizonEvents[t1]
                        tpChoices2 = self.horizonEvents[t2]
                        if gp.id in tpChoices1 and gp.id in tpChoices2:
                            dist = abs(t2 - t1)
                            if 0 < dist and dist <= (self.maxFollowUpSeparationMinutes * 60):
                                f1 = t1 if t1 < t2 else t2
                                f2 = t2 if t1 < t2 else t1
                                pair = (f1, f2)
                                if pair not in result:
                                    result.append(pair)
        return result

    def getErrTableChoices(self, satId, gp, t1, t2, t1ErrChoices, t2ErrChoices):
        gpType = dshieldUtil.getErrorTableTypeFromBiomeType(gp.type)

        if gpType not in self.errorTable:
            print("getErrTableChoices() ERROR! gpType not in errorTable: "+str(gpType)+" for gp: "+str(gp))
            self.unknownBiomeTypes.add(gpType)
            # unknown types:
            # 2 Evergreen Broadleaf Forest
            # 4 Deciduous Broadleaf Forest
            # 5 Mixed Forests
            # 6 Closed Shrublands
            # 9 Savannas
            # 10 Grasslands
            # 14 Cropland and Natural Mosaic

            # error table types:
            # biome type  1 = Evergreen Needleleaf Forest
            # biome type  7 = Open Shrublands
            # biome type  8 = Woody Savannas
            # biome type 12 = Croplands
            # biome type 16 = Bare
            gpType = 7
        gpErrTable = self.errorTable[gpType]
        isSingleton = False if t2 else True
        table = []
        L1 = t1ErrChoices["L"] if "L" in t1ErrChoices else []
        L2 = t2ErrChoices["L"] if t2ErrChoices and "L" in t2ErrChoices else []
        P1 = t1ErrChoices["P"] if "P" in t1ErrChoices else []
        P2 = t2ErrChoices["P"] if t2ErrChoices and "P" in t2ErrChoices else []

        # single observations
        obsCount = 1
        if isSingleton:
            for choice in L1:
                errRowIndex = (choice, 0)
                row = {"satId": satId, t1: {"L": choice}, "row": errRowIndex, "obs": obsCount, "err": dshieldUtil.getError(errRowIndex, gpErrTable)}
                table.append(row)
            for choice in P1:
                errRowIndex = (0, choice)
                row = {"satId": satId, t1: {"P": choice}, "row": errRowIndex, "obs": obsCount, "err": dshieldUtil.getError(errRowIndex, gpErrTable)}
                table.append(row)
        else:
            for choice in L1:
                errRowIndex = (choice, 0)
                row = {"satId": satId, t1: {"L": choice}, t2: None, "row": errRowIndex, "obs": obsCount, "err": dshieldUtil.getError(errRowIndex, gpErrTable)}
                table.append(row)
            for choice in P1:
                errRowIndex = (0, choice)
                row = {"satId": satId, t1: {"P": choice}, t2: None, "row": errRowIndex, "obs": obsCount, "err": dshieldUtil.getError(errRowIndex, gpErrTable)}
                table.append(row)
            for choice in L2:
                errRowIndex = (choice, 0)
                row = {"satId": satId, t1: None, t2: {"L": choice}, "row": errRowIndex, "obs": obsCount, "err": dshieldUtil.getError(errRowIndex, gpErrTable)}
                table.append(row)
            for choice in P2:
                errRowIndex = (0, choice)
                row = {"satId": satId, t1: None, t2: {"P": choice}, "row": errRowIndex, "obs": obsCount, "err": dshieldUtil.getError(errRowIndex, gpErrTable)}
                table.append(row)

        # double observations
        # L1 + P1
        obsCount = 2
        L1P1 = [(x,y) for x in L1 for y in P1]
        for (lChoice, pChoice) in L1P1:
            row = {"satId": satId, t1: {"L": lChoice, "P": pChoice}}
            if not isSingleton:
                row[t2]: None
            row["row"] = (lChoice, pChoice)
            row["err"] = dshieldUtil.getError(errRowIndex, gpErrTable)
            row["obs"] = obsCount
            table.append(row)

        if not isSingleton:
            # look up extended err codes whenever the same instrument is used more than once

            # L1 + L2, look up extended err code for L
            L1L2 = [(x,y) for x in L1 for y in L2]
            for (l1Choice, l2Choice) in L1L2:
                code = dshieldUtil.getExtendedErrCode((l1Choice, l2Choice))
                errRowIndex = (code, 0)
                row = {"satId": satId, t1: {"L": l1Choice}, t2: {"L": l2Choice}, "row": errRowIndex, "obs": obsCount, "err": dshieldUtil.getError(errRowIndex, gpErrTable)}
                table.append(row)

            # L1 + P2
            L1P2 = [(x,y) for x in L1 for y in P2]
            for (l1Choice, p2Choice) in L1P2:
                errRowIndex = (l1Choice, p2Choice)
                row = {"satId": satId, t1: {"L": l1Choice}, t2: {"P": p2Choice}, "row": errRowIndex, "obs": obsCount, "err": dshieldUtil.getError(errRowIndex, gpErrTable)}
                table.append(row)

            # P1 + L2
            P1L2 = [(x,y) for x in P1 for y in L2]
            for (p1Choice, l2Choice) in P1L2:
                errRowIndex = (p1Choice, l2Choice)
                row = {"satId": satId, t1: {"P": p1Choice}, t2: {"L": l2Choice}, "row": errRowIndex, "obs": obsCount, "err": dshieldUtil.getError(errRowIndex, gpErrTable)}
                table.append(row)

            # P1 + P2, Look up extended err code for P
            P1P2 = [(x,y) for x in P1 for y in P2]
            for (p1Choice, p2Choice) in P1P2:
                code = dshieldUtil.getExtendedErrCode((p1Choice, p2Choice))
                errRowIndex = (0, code)
                row = {"satId": satId, t1: {"P": p1Choice}, t2: {"P": p2Choice}, "row": errRowIndex, "obs": obsCount, "err": dshieldUtil.getError(errRowIndex, gpErrTable)}
                table.append(row)

            # L2 + P2
            L2P2 = [(x,y) for x in L2 for y in P2]
            for (l2Choice, p2Choice) in L2P2:
                errRowIndex = (l2Choice, p2Choice)
                row = {"satId": satId, t1: None, t2: {"L": l2Choice, "P": p2Choice}, "row": errRowIndex, "obs": obsCount, "err": dshieldUtil.getError(errRowIndex, gpErrTable)}
                table.append(row)

            # triple observations

            # L1 + P1 + L2, Look up extended err code for L
            obsCount = 3
            for (l1Choice, p1Choice) in L1P1:
                for l2Choice in L2:
                    lCode = dshieldUtil.getExtendedErrCode((l1Choice, l2Choice))
                    errRowIndex = (lCode, p1Choice)
                    row = {"satId": satId, t1: {"L": l1Choice, "P": p1Choice}, t2: {"L": l2Choice}, "row": errRowIndex, "obs": obsCount, "err": dshieldUtil.getError(errRowIndex, gpErrTable)}
                    table.append(row)

            # L1 + P1 + P2, Look up extended err code for P
            for (l1Choice, p1Choice) in L1P1:
                for p2Choice in P2:
                    pCode = dshieldUtil.getExtendedErrCode((p1Choice, p2Choice))
                    errRowIndex = (l1Choice, pCode)
                    row = {"satId": satId, t1: {"L": l1Choice, "P": p1Choice}, t2: {"P": p2Choice}, "row": errRowIndex, "obs": obsCount, "err": dshieldUtil.getError(errRowIndex, gpErrTable)}
                    table.append(row)


            # L1 + L2 + P2, Look up extended err code for L
            for (l1Choice, l2Choice) in L1L2:
                for p2Choice in P2:
                    lCode = dshieldUtil.getExtendedErrCode((l1Choice, l2Choice))
                    errRowIndex = (lCode, p2Choice)
                    row = {"satId": satId, t1: {"L": l1Choice}, t2: {"L": l2Choice, "P": p2Choice}, "row": errRowIndex, "obs": obsCount, "err": dshieldUtil.getError(errRowIndex, gpErrTable)}
                    table.append(row)

            # P1 + L2 + P2, Look up extended err code for P
            for (p1Choice, l2Choice) in P1L2:
                for p2Choice in P2:
                    pCode = dshieldUtil.getExtendedErrCode((p1Choice, p2Choice))
                    errRowIndex = (l2Choice, pCode)
                    row = {"satId": satId, t1: {"P": p1Choice}, t2: {"L": l2Choice, "P": p2Choice}, "row": errRowIndex, "obs": obsCount, "err": dshieldUtil.getError(errRowIndex, gpErrTable)}
                    table.append(row)

            # quadruple observations
            # L1 + P1 + L2 + P2, Look up extended codes for L and P

            obsCount = 4
            for (l1Choice, p1Choice) in L1P1:
                for l2Choice in L2:
                    lCode = dshieldUtil.getExtendedErrCode((l1Choice, l2Choice))
                    for p2Choice in P2:
                        pCode = dshieldUtil.getExtendedErrCode((p1Choice, p2Choice))
                        errRowIndex = (lCode, pCode)
                        row = {"satId": satId, t1: {"L": l1Choice, "P": p1Choice}}
                        if t2:
                            row[t2] =  {"L": l2Choice, "P": p2Choice}
                        row["row"] = errRowIndex
                        row["err"] = dshieldUtil.getError(errRowIndex, gpErrTable)
                        row["obs"] = obsCount
                        table.append(row)

        if self.includeChoiceErrors:
            for row in table:
                row["score"] = dshieldUtil.getNormalizedScore(row["err"])
        table.sort(key=lambda k: (k['err'], k['obs']))
        return table

    def setGpChoicesForFollowUpPair(self, gp, followUpPair):
        # TODO: handle multi-sat

        # NOTE: currently only considers the first pair
        # TODO: handle multiple follow-up combinations
        # print("\nsetGpChoicesForFollowUps() enter gp: "+str(gp))

        t1, t2 = followUpPair
        # print("setGpChoicesForFollowUpPair() t1: "+str(t1)+", t2: "+str(t2))
        t1PointingChoices = self.horizonEvents[t1][gp.id]
        t2PointingChoices = self.horizonEvents[t2][gp.id]
        if 1 in t1PointingChoices:
            t1PointingChoices["L"] = t1PointingChoices.pop(1)
        if 2 in t1PointingChoices:
            t1PointingChoices["P"] = t1PointingChoices.pop(2)
        if 1 in t2PointingChoices:
            t2PointingChoices["L"] = t2PointingChoices.pop(1)
        if 2 in t2PointingChoices:
            t2PointingChoices["P"] = t2PointingChoices.pop(2)
        gp.pointingChoices = {t1: t1PointingChoices, t2: t2PointingChoices}
        t1ErrChoices = self.convertPointingOptsToErrCodes(t1PointingChoices)
        t2ErrChoices = self.convertPointingOptsToErrCodes(t2PointingChoices)
        gp.errorChoices = {t1: t1ErrChoices, t2: t2ErrChoices}
        errTableChoices = self.getErrTableChoices(gp, t1, t2, t1ErrChoices, t2ErrChoices)
        if errTableChoices:
            gp.errorTableChoices = errTableChoices

    def setAllGpChoicesForSingleAccessTime(self, gp, tp):
        for sat in self.allSats:
            satId = sat.id
            satHorizonEvents = self.horizonEvents[satId]
            if tp in satHorizonEvents: # only some sats have events at tp
                self. setSatGpChoicesForSingleAccessTime(satId, gp, tp)

    def setSatGpChoicesForSingleAccessTime(self, satId, gp, tp):
        # print("\nsetSatGpChoicesForSingleAccessTime() tp: "+str(tp)+", gp: "+str(gp))
        satHorizonEvents = self.horizonEvents[satId]
        satHorizonEventsForTp = satHorizonEvents[tp]
        if gp.id not in satHorizonEventsForTp:
            # print("setSatGpChoicesForSingleAccessTime() gp "+str(gp.id)+" not in satHorizonEventsforTp: "+str(satHorizonEventsForTp))
            return
        pointingChoices = satHorizonEventsForTp[gp.id]
        if 1 in pointingChoices:
            pointingChoices["L"] = pointingChoices.pop(1)
        if 2 in pointingChoices:
            pointingChoices["P"] = pointingChoices.pop(2)
        if not gp.pointingChoices:
            gp.pointingChoices = {}
        gp.pointingChoices[tp] = pointingChoices
        if self.restrictPointingOptions:
            if not gp.errorChoices:
                gp.errorChoices = {}
            errChoices = self.convertPointingOptsToErrCodes(pointingChoices)
            gp.errorChoices[tp] = errChoices
            errTableChoices = self.getErrTableChoices(satId, gp, tp, None, errChoices, None)
            if errTableChoices:
                if not gp.errorTableChoices:
                    # TODO: change gp.errorTableChoices into dict with satId key
                    gp.errorTableChoices = []
                gp.errorTableChoices.extend(errTableChoices)

    def convertPointingOptsToErrCodes(self, tpChoices):
        lChoices = []
        pChoices = []
        if "L" in tpChoices:
            for lChoice in tpChoices["L"]:
                code = dshieldUtil.getErrorTableCode(lChoice)
                if code not in lChoices:
                    lChoices.append(code)
        if "P" in tpChoices:
            for pChoice in tpChoices["P"]:
                code = dshieldUtil.getErrorTableCode(pChoice)
                if code not in pChoices:
                    pChoices.append(code)
        lChoices.sort()
        pChoices.sort()
        result = {"L": lChoices, "P": pChoices}
        return result

    def flattenGpChoices(self, gp, gpEvents):
        choicesOut = []
        for payload in gpEvents.keys():
            payloadName = "L" if payload == 1 else "P"
            choicesIn = gpEvents[payload]
            # print("choiceIn: "+str(choiceIn))
            for option in choicesIn:
                choice = (payloadName, option)
                if choice not in choicesOut:
                    choicesOut.append(choice)
        choicesOut = sorted(choicesOut)
        return choicesOut

    def flattenChoices(self, tp, tpEvents):
        # print("flattenChoices() events: "+str(tpEvents))
        initialChoiceCount = 0
        choices = {}
        for gp in tpEvents.keys():
            gpEvents = tpEvents[gp]
            for payload in gpEvents.keys():
                choiceIn = gpEvents[payload]
                # print("choiceIn: "+str(choiceIn))
                for pointingOption in choiceIn:
                    initialChoiceCount += 1
                    choiceId = "'"+str(payload)+"."+str(pointingOption).zfill(2)+"'"
                    if choiceId in choices:
                        gpList = choices[choiceId]
                        if gp not in gpList:
                            gpList.append(gp)
                    else:
                        gpList = [gp]
                    choices[choiceId] = sorted(gpList)
        finalChoiceCount = len(choices.keys())
        self.choiceCounts[tp] = {"initial": initialChoiceCount, "final": finalChoiceCount, "diff": initialChoiceCount - finalChoiceCount}
        return choices

    def addChoiceScores(self, tp, tpChoices):
        for choice in tpChoices.keys():
            payload, pointingOption = choice.strip("'").split(".")
            pointingOption = int(pointingOption)
            gpList = tpChoices[choice]
            choiceScore = self.calculateChoiceScore(tp, payload, pointingOption, gpList)
            # append the score
            gpList.append(round(choiceScore, 4))

    def calculateChoiceScore(self, tp, payload, pointingOption, gpList):
        # Returns the sum of scores from the gp.errorTable for each gp in list
        # Pseudocode:
        #  for each gp in gpList:
        #    for each gpChoice in gpErrorTable:
        #        if gpChoice is a single obs: # TODO handle multiple obs
        #              if tp is in gpChoice:
        #                    gpChoicePayload, gPChoiceError = getPayloadAndError(gpChoice)
        #                    if gpChoicePayload and gpChoiceError match payload and pointingOpt:
        #                           choiceScore += getGpChoiceScore(gpChoice)
        #
        choiceScore = 0
        for gpi in gpList:
            if gpi > 0:
                gp = self.getGP(gpi)
                errorTableCode = dshieldUtil.getErrorTableCode(pointingOption)
                errorTableChoices = gp.errorTableChoices
                if not errorTableChoices:
                    print("calculateChoiceScore() ERROR! errorTableNotFound! for tp: "+str(tp)+", payload: "+str(payload)+", pointingOpt: "+str(pointingOption))
                    return None
                applicableChoices = []
                for tableChoice in errorTableChoices:
                    # errorTableChoices:
                    #    {13119: {'L': 2}, 18759: {'L': 1}, 'row': (7, 0), 'err': 0.003, 'obs': 2, 'score': 0.925}
                    #    {13119: {'L': 2, 'P': 2}, 18759: {'L': 1}, 'row': (7, 2), 'err': 0.003, 'obs': 3, 'score': 0.925}
                    #    {13119: {'L': 2, 'P': 3}, 18759: {'P': 2}, 'row': (2, 9), 'err': 0.012, 'obs': 3, 'score': 0.7}

                    if tableChoice["obs"] == 1:  # TODO: handle multiple obs
                        if tp in tableChoice and tableChoice[tp]:
                            choicePayload = list(tableChoice[tp].keys())[0]
                            choiceCode = tableChoice[tp][choicePayload]
                            if payload == choicePayload and errorTableCode == choiceCode:
                                applicableChoices.append(tableChoice)
                if len(applicableChoices) == 1:
                    choiceScore += applicableChoices[0]["score"]  # extract score val from errTableChoices
                else:
                    print("calculateChoiceScore() ERROR! multiple choices for tp: "+str(tp)+", payload: "+str(payload)+", pointingOpt: "+str(pointingOption))
        return choiceScore #  * -1 # make negative to distinguish from gpi

    def readPayloadAccessFileNewFormat2022(self, sat, payloadId):
        payloadName = "lsar" if payloadId == 1 else "psar"
        payloadName += str(sat.id)
        fileSuffix = dshieldUtil.convertDateTimeToFilenameFormat(self.inputFileDate, self.horizonId)
        payloadFilename = payloadName + "_"+fileSuffix
        dataPath = self.dataPath + "operator/orbit_prediction/RUN001/" + "sat"+str(sat.id)+"/access/"+payloadName+"/"
        dataPath += payloadFilename
        # dataPath = self.dataPath + "swarm/" + "sat"+str(sat.id)+"/"+payloadFilename
        payload = sat.payload1 if payloadId == 1 else sat.payload2
        print("\nReading raw data for s"+str(sat.id) +" payload "+str(payloadId)+ " from: "+payloadFilename)
        filename = dataPath
        if not os.path.exists(filename):
            print("\nreadPayloadAccessEvents() sat: "+str(sat.id)+" ERROR! File not found: " + filename + "\n")
            return
        lineCount = 0
        firstLine = True
        header = []
        with open(filename, "r") as f:
            lineNumber = 0
            for line in f:
                line = line.strip()
                lineNumber += 1
                if 1 <= lineNumber and lineNumber <= 3:
                    header.append(line)
                    continue
                elif lineNumber == 4:
                    continue
                line = line.strip()
                if line:
                    terms = line.split("\t")
                    eventTime = int(terms[0])
                    eventTime += self.horizonStart
                    gpi = int(terms[1])
                    pntOpts = terms[3][1:-1]
                    pointingOpts = list()
                    pntOpts = pntOpts.split(",")
                    for opt in pntOpts:
                        if not opt:
                            print("ERROR: no pointing opt")
                        pointingOpts.append(int(opt))
                        pointingOpts.sort()
                    accessInfo = {"time": eventTime, "gpi":gpi, "pointingOpts": pointingOpts}
                    if eventTime in payload.accessTimes:
                        payload.accessTimes[eventTime] += 1
                    else:
                        payload.accessTimes[eventTime] = 1
                    payload.accessEvents.append(accessInfo)
                    # update gp accessTimes
                    gp = self.getGP(gpi)
                    if self.imageLock:
                        eventTime -= 1
                    if eventTime not in gp.accessTimes:
                        gp.accessTimes.append(eventTime)
                    lineCount += 1
                    if lineCount % 10000 == 0:
                        print("  line: "+str(lineCount))
        payload.accessEvents = sorted(payload.accessEvents, key=lambda k: (k['time'], k['gpi']))
        payload.header = header

    def copyEclipseFilesToPreprocessingFolders(self, satList):
        for satId in satList:
            # fileSuffix = dshieldUtil.convertDateTimeToFilenameFormat(self.inputFileDate, self.horizonId)
            dataPathIn = self.dataPath + "operator/orbit_prediction/RUN001/" + "sat"+str(satId)+"/eclipse/"
            assert os.path.exists(dataPathIn), "copyEclipseFilesToPreprocessingFolders() ERROR! path not found: "+dataPathIn
            files = os.listdir(dataPathIn)
            destFolder = dshieldUtil.getPrepPathForSat(satId)
            for file in files:
                srcFile = dataPathIn+file
                destFile = destFolder+"s"+str(satId)+"."+file
                shutil.copyfile(srcFile, destFile)


    def readGpModelErrFile(self):
        filepath = self.dataPath + "planner/preprocessing/gpModelErr.txt"
        horizonTimes = None
        if self.horizonId == 1:
            horizonTimes = (0,3)
        elif self.horizonId == 2:
            horizonTimes = (6,9)
        elif self.horizonId == 3:
            horizonTimes = (12, 15)
        elif self.horizonId == 4:
            horizonTimes = (18, 21)
        print("readGpModelErrFile() file: "+str(filepath))
        if os.path.exists(filepath):
            lineCount = 0
            print("reading gp model err file: "+filepath)
            with open(filepath, "r") as f:
                for line in f:
                    line = line.strip()
                    gpInfo = ast.literal_eval(line)
                    gpi = gpInfo['gp']
                    if gpi not in self.gpModelErrors:
                        self.gpModelErrors[gpi] = []
                    errTimes = gpInfo['modelErr']
                    for errTime in errTimes:
                        time = errTime[0]
                        err = errTime[1]
                        if time in horizonTimes:
                            self.gpModelErrors[gpi].append(errTime)
                    lineCount += 1
                    if lineCount % 100000 == 0:
                        print("line: "+str(lineCount))
        print("readGpModelErrFile() exit")



    def printGpAccessSummary(self):
        # all GP in 24 hour window
        # {'gp': 0, 'type': 'Bare', 'lat': 83.3252125094589, 'lon': -76.3226141078839, 'filteredAccessTimes': [(1511, 'noRain'), (5283, 'noRain'), (10931, 'noRain'), (16573, 'noRain')]}
        i = 0
        maxAccessTimeCount = 0
        totalAccessTimeCount = 0
        gpKeys = self.gpDict.keys()
        gpCount = len(gpKeys)
        singleAccessCount = 0
        noAccessCount = 0
        for gpi in gpKeys:
            gp = self.gpDict[gpi]
            gpAccessTimeCount = len(gp.accessTimes)
            if gpAccessTimeCount > 0:
                if gpAccessTimeCount == 1:
                    singleAccessCount += 1
                    self.singleAccessGP.append(gpi)
                totalAccessTimeCount += gpAccessTimeCount
                if gpAccessTimeCount > maxAccessTimeCount:
                    maxAccessTimeCount = gpAccessTimeCount
                    self.maxAccessGP = gpi
            else:
                noAccessCount += 1
            if len(gp.rainHours) > 0 and i < 25:
                print("GP "+str(gp))
                i += 1

        avgAccessTimeCount = totalAccessTimeCount / gpCount
        print("GP count: "+str(gpCount)+", avgAccessTimeCount/GP: "+str(avgAccessTimeCount)+", max: "+str(maxAccessTimeCount)+", singleAccessCount: "+str(singleAccessCount)+", noAccessCount: "+str(noAccessCount))
        print("maxAccessGP: "+str(self.maxAccessGP))

    def writeHorizonGpFile(self):
        filename = str(self.horizonStart) + "-" + str(self.horizonEnd) + ".gp.txt"
        filepath = self.dataPath+"planner/preprocessing/"
        filepath += filename
        sortedHorizonGPs = list(self.horizonGPs)
        sortedHorizonGPs.sort(key=lambda x: x.id)
        print("writeHorizonGpFile() file: "+filepath+", GP count: "+str(len(sortedHorizonGPs)))
        self.gpWithErrChoicesCount = 0
        self.gpWithSingleObsBest = 0
        self.gpWithSingleObsSecondBest = 0
        lineNumber = 0
        with open(filepath, "w") as f:
            for gp in sortedHorizonGPs:
                if gp.id in self.gpModelErrors.keys():
                    gp.initialModelError = self.gpModelErrors[gp.id]
                else:
                    print("writeHorizonGpFile() ERROR! gp not found in model errors: "+str(gp.id))
                f.write(str(lineNumber)+": "+str(gp)+"\n")
                # f.write(str(lineNumber)+": "+gp.prettyPrint())
                lineNumber += 1
                if gp.errorTableChoices:
                    self.gpWithErrChoicesCount += 1
                    choiceCount = len(gp.errorTableChoices)
                    self.totalErrChoiceCount += choiceCount
                    if not self.maxErrChoiceGP or choiceCount > len(self.maxErrChoiceGP.errorTableChoices):
                        self.maxErrChoiceGP = gp
                    bestChoice = gp.errorTableChoices[0]
                    if bestChoice["obs"] == 1:
                        self.gpWithSingleObsBest += 1
                    elif len(gp.errorTableChoices) > 1:
                        secondBest = gp.errorTableChoices[1]
                        if secondBest["obs"] == 1:
                            if secondBest["err"] - bestChoice["err"] < self.errorDiffThreshold:
                                self.gpWithSingleObsSecondBest += 1
        # end with open file
        print("wrote "+str(lineNumber)+" gp to file")

    def getSat(self, satId):
        for sat in self.allSats:
            if sat.id == satId:
                return sat

    def getGP(self, index):
        if index not in self.gpDict:
            print("getGP() ERROR! gp not found: "+str(index))
        else:
            return self.gpDict[index]

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

    def initFileFolders(self, satList):
        prepPath = self.dataPath+"planner/preprocessing"
        if not os.path.exists(prepPath):
            os.mkdir(prepPath)
        for satId in satList:
            p = dshieldUtil.getPrepPathForSat(satId)
            if not os.path.exists(p):
                os.mkdir(p)

def main():
    obsPreprocessor = ObsPreprocessor()
    satList = [1,2,3] #,2,3]
    inputFileDate = datetime.date(2020,1,4) # y,m,d = 1/4/2022
    horizonId = 4
    obsPreprocessor.start(satList, inputFileDate, horizonId)

if __name__ == '__main__':
    main()
