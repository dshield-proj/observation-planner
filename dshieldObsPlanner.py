import ast
import datetime
from planit import Planit
from gp import GP
import time
import os
from decimal import Decimal
import dshieldUtil
import math
import json
import copy
import gc

def main():

    # settings
    satList = [1,2,3]
    horizonId = 3
    maxTick = None
    strategy = "beam.3"  #dfs # beam.2
    inputFileDate = datetime.date(2020,1,5) # y,m,d = 1/4/2022
    experimentRun = "RUN002" #"RUN001"
    # main processing
    obsPlanner = ObsPlanner(satList, inputFileDate, horizonId, experimentRun, maxTick, strategy)
    obsPlanner.statsInit()
    obsPlanner.createInitialPlan()
    obsPlanner.executePlan(obsPlanner.plan)
    obsPlanner.analyzeResults()
    obsPlanner.updateStats(obsPlanner.successNode)
    print(obsPlanner.printStats())
    print("main done")


class ObsPlanner:
    def __init__(self, satList, inputFileDate, horizonId, experimentRun, maxTick, strategy):
        # TODO: replace these data paths with your path to where you downloaded the data
        # self.dataPath = "/Users/richardlevinson/DshieldDemoData2022_Run1/planner/"
        # self.demoDataPath = "/Users/richardlevinson/DshieldDemoData2022_Run1/"
        self.dataPath = "/Users/richardlevinson/DshieldDemoData2022_Run2/planner/"
        self.demoDataPath = "/Users/richardlevinson/DshieldDemoData2022_Run2/"
        # config params (set in start())
        # planner config params
        beamwidth = 1
        if strategy.startswith("beam"):
            terms = strategy.split(".")
            strategy = terms[0]
            beamwidth = int(terms[1])
        elif strategy != "dfs":
            print("ERROR! Invalid strategy: "+str(strategy))
            return
        self.nodeSorterHeuristic = strategy #"beam" #"dfs"
        self.valSelectorHeuristic = "maxErrReduction" #"maxGpCount" #"gpRankedChoice" #"maxErrReduction" #"maxErrReduction" # "maxGpCount" #maxGpRankedChoice #"minGpChoiceErr"  #"maxGpChoiceScore" #"maxGpCount" #maxChoiceScore"
        self.horizonDur = 21600
        self.experimentRun = experimentRun
        self.maxTick =  maxTick
        self.useSortedGP = False
        self.sortedGPpct = 0.15
        self.planner = Planit()
        self.planner.nodeBeamWidth = beamwidth
        self.fixedPointingOption = False
        self.voteLogging = False
        self.gapPlanningEnabled = False
        self.multipleRootNodes = False

        self.satList = satList
        self.inputFileDate = inputFileDate
        self.horizonId = horizonId  # 1 2 3 4
        self.horizonStart = ((self.horizonId - 1) * self.horizonDur) # + 1 #21601 #1 #21601  #1
        self.horizonEnd = self.horizonStart + self.horizonDur - 1
        # self.includeChoiceErrors = False # used by udpateState()
        # self.removeNewObs =      True  # remove newly observed GP from future choices
        # self.removeOldObs =      True  # remove GP previously observed in prior horizons
        self.imageLock =         True  # Hold each obs for 3 seconds (accessTime +/- 1 sec)
        self.blockoutSlewTimes = True  # remove vars with infeasible slew times
        self.createCommandsFile = False # export commands for visualization
        self.printResults =      False # True to print summary results
        self.plan = None # TODO: Is this used?
        self.successNode = None
        self.initialPlan = None
        self.initialPlanSuccessNode = None
        self.initialPlanStepCount = 0
        self.initialPlanNodeCount = 0
        self.initialPlanHorizonGPs = None
        self.initialPlanObservedGPs = None
        self.initialModelErrSummary = {}
        self.initialHorizonGpErrAvg = 0
        self.initialObservedGpErrAvg = 0
        self.initialObservedGpErrTotal = 0
        self.initialObservedGpErrCount = 0
        self.initialPlanRewardTotal = 0
        # self.initialPlanSatPlans = None
        self.finalHorizonGpErrAvg = 0
        self.isGapPlan = None
        self.gapPlanRootNodeId = None # first node id for gap filling plan
        self.satEvents = {}
        self.satEventHeaders = {} # from payload access files
        self.satEclipses = {}
        self.initialEnergy = None
        self.maxBatteryCharge = None
        self.energyMax = None
        self.energyMin = None
        self.powerIn  = None
        self.stats = {}
        self.state = {}
        self.errorTable = {}
        self.slewTable = {}
        # self.horizonEvents = {}
        self.initialHorizonGpErrTotal = 0 #TODO: does this need to be copied for gap planner?
        self.horizonGPs = set()  #TODO: does this need to be copied for gap planner?
        self.sortedHorizonGPs = []
        self.sortedHorizonGPerr = {}
        self.observedGPs = None
        self.gpDict = {}
        self.singleAccessGP = []
        self.maxAccessGP = None
        self.removedChoices = 0
        self.removedVars = 0
        self.initialVarCount = 0
        self.priorGPs = []
        self.priorHorizonFinalTick = None
        self.priorHorizonFinalAngle = None
        # self.gpScores = {}
        self.plannedGpCount = 0
        # self.planScore = 0
        self.gpErrResults = {}
        self.solutionSummary = {}
        self.skippedCandidates = 0
        self.lowestEnergy = {}

        self.gaps = {}
        self.satGaps = {}
        self.sensorStates = {}
        self.initSensorStatesAndGaps()
        self.rowRewards = 0

        # setup solver
        self.planner.setNodeSorter(self.nodeSorter)
        self.planner.setVarSelector(self.varSelector)
        self.planner.setValSorter(self.valSorter)
        # self.planner.setNodeSelector(self.nodeSelector)
        # self.planner.setValSelector(self.valSelector)
        self.planner.setChoicePropagator(self.propagateChoice)
        # self.planner.setGapStartPropagator(self.propagatePreGapChoice)
        # self.planner.setGapEndPropagator(self.ensureSlewTimeToGapEnd)
        self.planner.setStateUpdater(self.updateState)
        # self.planner.setSuccessTest(self.isGoalState)
        self.planner.storeNodePlans = True
        # self.planner.setNodeScoringMethod(self.setNodeScore)
        print("ObsPlanner() constructor, horizonId: "+str(self.horizonId)+": "+str(self.horizonStart)+"-"+str(self.horizonEnd))


            # TODO: could aggregate using avg vs. maxs
            # one of: maxChoiceScore, maxGpCount, maxRareGP, maxGpChoiceScore, maxGpRank
            #   maximizeChoiceScore = select val which maximizes scores for covered GP
            #   maximizeGpCount = select val which maximized # of covered GP
            #   maximizeRareGp = select val which covers the most GP which have only one access time (most constrained)
            #   maximizeGpChoiceScore = select val which maximizes GP * gpChoiceScore
            #   maxGpNormalizedChoiceScore = select val which maximizes GP * (gpChoiceScore/maxGpChoiceScore)
            #   maximizeGpRank = select val which maximizes each GP's choice rank (prefer each GP's 1st choice, then 2nd, etc).

    def initSensorStatesAndGaps(self):
        for satId in self.satList:
            self.sensorStates[satId] = {"L": {}, "P": {}}
            self.satGaps[satId] = {}

    def createInitialPlan(self):
        self.isGapPlan = False
        self.readEclipseFiles()
        self.initEnergyModel()
        self.loadPreprocessingResults()
        self.createDecisionVars()
        self.createInitialState()
        if self.multipleRootNodes:
            self.planner.createMultipleRootNodes()
            print("Initial Tree:")
            print(str(self.planner.printTree(None, 0)))
        self.solveIt()
        # print("Final Tree:")
        # print(str(self.planner.printTree(None, 0)))
        # if self.valSelectorHeuristic == "gpRankedChoice":
        #     print("skipped candidates pass 1: "+str(self.skippedCandidates))

    # experiment data collection
        # read data
        # self.comparePlans("s11.1-21600.rainOnly.plan.good.txt", "s11.1-21600.rainOnly.plan.new.txt")
        # if self.printResults:
        #     self.analyzeResults()
        #     return
        # self.verifyGPaccessTimes()

    def initializeEvents(self):
        #self.horizonGPs.clear()
        # if self.removeOldObs:
        #     self.priorGPs.clear()
        self.readHorizonGpFile()
        for satId in self.satList:
            # TODO: readFlatHorizonFile doesn't create "combos" for multi-obs
            self.readFlatHorizonFile(satId)
            # self.satEvents[satId] = self.horizonEvents
        for hId in range(1,self.horizonId):
            self.readPriorObservations(self.satList[0], hId)

    def createDecisionVars(self):
        print("createDecisionVars()")
        varCount = 0
        maxVarCount = None
        allEvents = []
        for satId in self.satEvents.keys():
            satEvents = self.satEvents[satId]
            print("createDecisionVars() s"+str(satId) +" events: "+str(len(satEvents)))
            filteredEvents = self.filterNegativeRewards(satEvents)
            self.satEvents[satId] = filteredEvents
            for event in filteredEvents:
                allEvents.append((satId, event))
        allEvents.sort(key = lambda x: x[1])
        for event in allEvents:
            satId = event[0]
            tp = event[1]
            varName = "s" + str(satId)+"." + str(tp)
            satEvents = self.satEvents[satId]
            choices = satEvents[tp]
            # choices = self.horizonEvents[tp]
            self.planner.addVar(varName, choices)
            varCount += 1
            if maxVarCount and varCount > maxVarCount:
                break

        self.planner.rootNode.unassignedVars.sort(key=lambda x: x.tick)
        print("createDecisionVars() created "+str(varCount)+" vars/"+str(len(self.planner.rootNode.unassignedVars)))
        # for v in self.planner.rootNode.unassignedVars:
        #     print(str(v))


    def filterNegativeRewards(self, satEvents):
        removedTpCount = 0
        removedGpCount = 0
        removedCmdCount = 0
        # filter gp
        # satEvents = copy.deepcopy(events)
        print("removing command choices with negative rewards")
        satTimes = list(satEvents.keys())
        for tick in satTimes:
            event = satEvents[tick]
            # event: {'L.33': [195912, 195913, 195914, 195915], 'L.34': [195910, 195911], 'P.32': [195914, 195915]}
            cmdChoices = list(event.keys())
            for cmd in cmdChoices:
                gpList = event[cmd]
                filteredGpList = []
                for gpId in gpList:
                    if not self.useSortedGP or gpId in self.sortedHorizonGPerr:
                        reward = self.getGpReward(gpId, tick, cmd)
                        if reward > 0:
                            filteredGpList.append(gpId)
                        else:
                            # print("filterNegativeRewards() removing gp: "+str(gpId))
                            removedGpCount += 1
                if filteredGpList:
                    event[cmd] = filteredGpList
                else:
                    # print("filterNegativeRewards() removing cmd: "+str(cmd))
                    event.pop(cmd)
                    removedCmdCount += 1
            if event:
                satEvents[tick] = event
            else:
                # print("filterNegativeRewards() removing tp: "+str(tick))
                removedTpCount += 1
                satEvents.pop(tick)

        print("filterNegativeRewards() removed gps: "+str(removedGpCount)+", removed cmds: "+str(removedCmdCount)+", removed tps: "+str(removedTpCount))
        return satEvents

    def solveIt(self):
        print("\nsolveIt()")
        self.successNode = None
        self.initialVarCount = len(self.planner.rootNode.unassignedVars)
        print("initial var count: "+str(self.initialVarCount))
        if self.priorGPs:
            print("Removing prior obs ("+str(len(self.priorGPs))+")")
            self.removeDuplicateObs(self.planner.rootNode, self.priorGPs, None)
            print("   removedChoices: "+str(self.removedChoices)+", removedVars: "+str(self.removedVars))
            newVarCount = len(self.planner.rootNode.unassignedVars)
            print("   var count after removing prior obs: " + str(newVarCount))
            self.initialVarCount = newVarCount
        # self.createInitialState()
        self.successNode = self.planner.solveIt()
        if self.successNode:
            print("Solution Found!")
            self.plan = self.collectPlan(self.successNode) # Used by executePlan()
            self.observedGPs = self.collectObservedGP(self.successNode) #self.successNode.state['observedGp']
            # obsGP = self.collectObservedGP(self.successNode)
            if self.isGapPlan:
                # combine initial plan and gap plan
                self.combinePlans()
            else:
                self.initialPlanSuccessNode = self.successNode
                self.initialPlanHorizonGPs = self.horizonGPs
                self.initialPlanObservedGPs = self.collectObservedGP(self.successNode) #.state['observedGp']
                self.initialPlanRewardTotal = self.successNode.planReward
                self.initialPlanNodeCount = self.planner.nextNodeId
                self.initialPlanStepCount = len(self.plan)

            self.stats["planId"] += 1
            print("\nFinal plan step: " + str(self.successNode))
            finalObs = self.observedGPs
            finalObsSet = set(finalObs)
            horizonGPcount = len(self.horizonGPs)
            observedGPcount = len(finalObsSet)
            missedGPcount = horizonGPcount - observedGPcount
            # finalScore = dshieldUtil.roundIt(self.successNode.state["planScore"])
            # avgGpScore = finalScore / len(finalObs)
            # avgGpScore = dshieldUtil.roundIt(avgGpScore)
            print("\nHeuristic: "+str(self.valSelectorHeuristic))
            msg = "\nHorizon GP count: "+str(horizonGPcount)
            if not len(finalObs) == horizonGPcount:
                msg += ", * * Observed GP count: " + str(len(finalObs))
            msg += ", Observed GP set count: " + str(observedGPcount) + ", missed GPs: " + str(missedGPcount)
            print(msg)
            # print("PlanScore: "+str(finalScore)+", avg score/GP: "+str(avgGpScore))
            print("\nInitial varCount: "+str(self.initialVarCount)+", removedVars: "+str(self.removedVars)+", removedChoices: "+str(self.removedChoices))
            timestamp = time.strftime("%b.%-d.%y.%H.%M", time.localtime())
            for satId in self.satEvents.keys():
                self.writePlanToFile(self.successNode, satId, timestamp)
                self.writePrettyPlanToFile(satId, timestamp)
                # self.writeHorizonGpPlanFile(satId)
                # self.writeGapsToFile(satId)
                # if self.createCommandsFile:
                #     self.writePlanCommandsFile(self.plan)

#=======================
#     Node State

    def createInitialState(self):
        state = {}
        self.planner.setInitialState(state)
        # if not self.isGapPlan: # not gap planning
        #     # satPlans  = {}
        #     satEnergy = {}
        #     # for satId in self.satList:
        #     #     satPlans[satId] = [] # list of plan steps (dicts)
        #     state.update({"satPlans": satPlans})
        # else: # gap planning
        #     # copy satPlans and satEnergy from initial plan
        #     # initialPlanSuccessNode = self.initialPlan[-1]
        #     # state['satPlans']  = self.initialPlanSuccessNode.state['satPlans']
        #     # state['observedGp'] = initialPlanSuccessNode.state['observedGp']

    def updateState(self, node, var, choiceInfo):
        # choice is pair: (option, gpList)
        # print("updateState() choice: "+str(choiceInfo))
        node.state['sat'] = var.satId
        node.state['tick'] = var.tick
        choice = choiceInfo[0]
        gpList = choiceInfo[1] #[:-1] # strip off the choice score (last item in gpList)
        # choiceScore = 0
        # if self.includeChoiceErrors: # true by default
        #     choiceScore = choiceInfo[1][-1]
        # for gpi in gpList:
        #     gp = self.getGP(gpi)
        #     gpChoiceScore = self.getGpChoiceScore(gp, var.tick, choice)
        #     gpChoiceRank = self.getGpChoiceRank(gp, var.tick, choice)
        #     gpNormalizedChoiceScore = self.getNormalizedGpChoiceScore(gp, var.tick, choice)
            # choiceDict = dshieldUtil.parseChoice(choice)
            # payload = choiceDict["payload"]
            # pointingOpt = choiceDict["pointingOption"]
            # errorCode = choiceDict["errorCode"]
            # # TODO: handle multiple observations for gp.planChoice
            # errorTableType = dshieldUtil.getErrorTableTypeFromBiomeType(gp.type)
            # if errorTableType not in self.errorTable:
            #     print("updateState() ERROR! unknown biome type for gp: "+str(gp))
            #     errorTableType = 7
            # choiceErr = dshieldUtil.getCmdErr(choice, self.errorTable[errorTableType])
            # gp.planChoice = {"tp": var.tick, "choice": choice, "err": choiceErr, "score": gpChoiceScore, "rank": gpChoiceRank, "rankedScore": gpNormalizedChoiceScore}

        # if isinstance(choice, int):
        #     choice = var.payload + "."+str(choice)
        self.updateNodeRewards(node, var, choice, gpList, "maxErrReduction")
        # self.updateSearchDepth(node)
        # self.updateSatPlanAndEnergy(node, choiceInfo[0])

        # node.state["planScore"] += choiceScore
        statusMsg = None
        return (True, statusMsg)


    def updateNodeRewards(self, node, var, cmd, gpList, heuristic):
        choiceReward = self.getCmdReward(var.tick, cmd, gpList, heuristic)
        node.choiceReward = choiceReward
        if node.parent:
            parentNode = self.planner.getNode(node.parent)
            parentPlanReward = parentNode.planReward
            node.planReward = parentPlanReward + choiceReward

    def collectObservedGP(self, node):
        observedGp = []
        done = False
        while not done:
            if node.var:
                choice = node.var.assignment  # assignment = tuple (cmd, gpList, reward)
                if choice:
                    gpList = choice[1]
                    observedGp.extend(gpList)
                    if node.parent:
                        node = self.planner.getNode(node.parent)
                        if not node.var:
                            done = True
                    else:
                        done = True
                else:
                    print("collectObservedGP() ERROR! no choice for node: "+str(node))
            else:
                print("collectObservedGP() ERROR! node missing var: "+str(node))
                done = True
        observedGp.sort()
        return observedGp

    #===========================
    #   Energy and Power Model

    def initEnergyModel(self):
        # energyMax = Cb = 160 Watt-hour = 160*3600 Joules =  576000 Joules - updated 9/14/21
        # energyMax = Cb = 484 Watt-hour = 484*3600 Joules = 1742400 Joules
        # energyMin = energyMax * 0.7 = 1219680
        # powerIn factors:
        #     1368 Watts/m2 = Energy from Sun per unit area
        #      0.3 = eta = solar efficiency
        #      0.7 = Id = degradation due to temperature and implementation
        #   45 deg = theta = assumed angle between solar panels and Sun-vector when sat is the bright side
        #  2.2 m^2 = Asa = solar-panel area on the (L+P) satellites
        #
        #  powerIn = 1388 * 0.3 * 0.7 * 2.2 * cos(45) Watts = 446.9028 Watts (when not in eclipse)
        #  powerIn (9/7/21) = 266 Watts(when not in eclipse)


        # scenario 1: 11/20/21
        self.maxBatteryCharge = 484 # Watt-hour
        self.powerIn          = 266 # Watts

        # scenario 2:
        # self.maxBatteryCharge = 350 # Watt-hour
        # self.powerIn          = 260 # Watts

        # scenario 3:
        # self.maxBatteryCharge = 250 # Watt-hour
        # self.powerIn          = 260 # Watts

        # scenario 4:
        # self.maxBatteryCharge = 160 # Watt-hour
        # self.powerIn          = 253 # Watts

        self.energyMax = self.maxBatteryCharge * 3600 # Jules
        self.initialEnergy = self.energyMax * 0.9
        self.energyMin     = self.energyMax * 0.7  # Joules
        # self.powerIn  = 446.9028 # Watts  (when not in eclipse)

    def getPowerIn(self, satId, firstTick, lastTick):
        result  = 0
        for tick in range(firstTick, lastTick +1):
            if not self.isSatInEclipse(satId, tick):
                result += self.powerIn # 266 W (updated 9/7/21)
        return result

    def getPowerOut(self, firstTick, lastTick, cmd, previousCmd):
        # idlePower        = 410.0 # Watts (from Vinay)
        # idlePower        = 260.0 # Watts (from Vinay)
        # idlePower        = 151.4 # Watts (from Ben)
        idlePower      = 181.0 # Watts (from Ben, updated 9/7/21)
        instrumentPower  =  100 # Watts per instrument
        downlinkingPower = 22.6 # Watts (not used in current model)
        maneuveringPowerDefault = 69 # Watts  (not used in current model)

        slewTime, slewEnergy = self.getSlewEnergyRequired(cmd, previousCmd)
        # Convert slew energy (Joules) to watts:  powerOut = Joules/slewTime
        slewPowerOut = slewEnergy / slewTime if slewTime > 0 else 0
        instrumentCount = cmd.count(".")
        instrumentPowerOut = instrumentCount * instrumentPower
        instrumentPowerOut *= 3

        idleTime = lastTick - firstTick + 1
        idlePowerOut = idleTime * idlePower
        result = idlePowerOut + instrumentPowerOut + slewPowerOut
        return result

    def updateEnergyForPlanStep(self, satId, planStep, priorStep):
        # caller getCombinedSatPlan()
        # calculate energy change between plan steps
        tick = planStep['tick']
        cmd = planStep['cmd']
        priorTick = priorStep['tick']
        priorEnergy = priorStep['energy']
        priorCmd = priorStep['cmd']
        powerIn = self.getPowerIn(satId, priorTick+1, tick)
        powerOut = self.getPowerOut(priorTick+1, tick, cmd, priorCmd)
        updatedEnergy = priorEnergy + powerIn - powerOut
        if updatedEnergy > self.energyMax:
            # print("updateEnergyStates() Battery Saturated! planStep: "+str(step))
            updatedEnergy = self.energyMax
        planStep.update({'energy': updatedEnergy})
        return planStep

    # def updateSatPlanAndEnergy(self, node, cmd):
    #     # caller updateState()
    #     nodeId = node.id
    #     satId = node.var.satId
    #     tick = node.var.tick
    #     # satPlan  = node.state['satPlans'][satId]
    #     planStep = {"node": nodeId, "tick": tick, "cmd": cmd}
    #     # if not satPlan:
    #     #     # set the initial energy for first planStep only
    #     #     planStep.update({"energy": self.initialEnergy})
    #     # satPlan.append(planStep)
    #     # if self.isGapPlan:
    #     #     # sort plan steps chronologically by tick
    #     #     sortedPlan = sorted(satPlan, key=lambda planStep: planStep['tick'])
    #     #     node.state['satPlans'][satId] = sortedPlan
    #
    #         # # find prior step after inserting (sorting) new step
    #         # previousSteps = self.collectPreviousPlanStepsSinceEnergyUpdate(nodeId, sortedPlan)
    #         #
    #         # print('updateSatPlansAndEnergy() satId: '+str(satId)+', node: '+str(nodeId)+ ', prior steps: '+str(previousSteps))
    #         # if previousSteps:
    #         #     self.updateEnergyStates(node, cmd, previousSteps)

    # def collectPreviousPlanStepsSinceEnergyUpdate(self, nodeId, sortedPlan):
    #     # collect last prior energy update through nodeId
    #     priorSteps = []
    #     lastEnergyUpdateStep = None
    #     for step in sortedPlan:
    #         if 'energy' in step:
    #             lastEnergyUpdateStep = step
    #         if 'energy' not in step:
    #             priorSteps.append(step)
    #         if step['node'] == nodeId:
    #             break
    #     priorSteps.insert(0, lastEnergyUpdateStep)
    #     return priorSteps

    # def updateEnergyStates(self, node, cmd, planSteps):
    #     satId = node.var.satId
    #     priorStep = planSteps.pop(0) # strip off last step with energy
    #     for step in planSteps:
    #         # calculate energy change between ticks
    #         tick = step['tick']
    #         priorTick = priorStep['tick']
    #         priorEnergy = priorStep['energy']
    #         powerIn = self.getPowerIn(satId, priorTick+1, tick)
    #         powerOut = self.getPowerOut(priorTick+1, tick, cmd, step['cmd'])
    #         updatedEnergy = priorEnergy + powerIn - powerOut
    #         if updatedEnergy > self.energyMax:
    #             # print("updateEnergyStates() Battery Saturated! planStep: "+str(step))
    #             updatedEnergy = self.energyMax
    #         step.update({'energy': updatedEnergy})
    #         priorStep = step

        # print("updateEnergyState() satId: "+str(satId)+', tick: '+str(tick)+", priorTick: "+str(priorTick)+", priorEnergy: "+str(priorEnergy)+", powerIn: "+str(powerIn)+", powerOut: "+str(powerOut)+", updatedEnergy: "+str(updatedEnergy))
        # nodeStateEnergy = node.state["energy"]
        # print("updateEnergyState() prior energy state s"+str(satId)+": "+str(nodeStateEnergy[satId]))
        # nodeStateEnergy[satId] = updatedEnergy
        # print("updateEnergyState() prior energy state s"+str(satId)+": "+str(nodeStateEnergy[satId]))

    def isSatInEclipse(self, satId, tick):
        if satId in self.satEclipses:
            eclipses = self.satEclipses[satId]
            for eclipse in eclipses:
                start = eclipse['start']
                end = eclipse['end']
                if start <= tick and tick <= end:
                    return True
        else:
            print("isSatInEclipse() ERROR! satId "+str(satId) +" not found")
        return False

    def getSlewEnergyRequired(self, cmd, previousCmd):
        slewTime, slewEnergy = 0, 0
        if previousCmd:
            fromAngle = self.getPointingAngleFromChoice(previousCmd)
            toAngle = self.getPointingAngleFromChoice(cmd)
            slewTableRow = self.slewTable[fromAngle]
            slewTime, slewEnergy = slewTableRow[toAngle]
        return slewTime, slewEnergy

    # def getNodeEnergy(self, nodeId, satPlan):
    #     planStep = self.getPlanStep(nodeId, satPlan)
    #     if planStep:
    #         if 'energy' in planStep:
    #             return planStep['energy']
    #     else:
    #         print("getNodeEnergy() ERROR! plan step missing!")

    def getSlewTimeAndEnergy(self, fromAngle, toAngle):
        slewTableRow = self.slewTable[fromAngle]
        slewTime, slewEnergy = slewTableRow[toAngle]
        slewTimeCeil = math.ceil(slewTime)
        return (slewTimeCeil, slewEnergy)

    def getPlanStep(self, nodeId, planSteps):
        for step in planSteps:
            if step['node'] == nodeId:
                return step

#=======================
#     Constraints

    def propagateChoice(self, node, var):
        self.removeDuplicateObs(node, var.assignment[1], var.tick)
        self.removeInfeasibleSlewChoices(node, var)

    def removeDuplicateObs(self, node, gpList, varTick):
        # remove all GP in GPlist from node var's future choices
        # varTick is currentTick when called for current horizon, None for previous horizon
        changedVars = []
        replacementVars = []
        for gpi in gpList:
            gp = self.getGP(gpi)
            if gp:
                gpTimes = gp.accessTimes
                if len(gpTimes) > 1:
                    for gpTime in gpTimes:
                        if not gpTime == varTick:
                            # multiple sats may have action at same time (though likely not same GP at same time)
                            otherVars = self.getVarsForTime(node, gpTime)
                            for otherVar in otherVars:
                                varIndex = node.unassignedVars.index(otherVar)
                                # handle case when the same var has multiple GP removed (need to propagate/accumulate the changes for each GP)
                                otherVarCopy = self.findReplacementVar(varIndex, replacementVars)
                                if otherVarCopy:
                                    otherVar = otherVarCopy
                                choicesToRemove = []
                                updatedGpLists = {}
                                for otherVarChoiceKey in otherVar.choices.keys():
                                    choiceGPlist = otherVar.choices[otherVarChoiceKey]
                                    if gpi in choiceGPlist:
                                        # print("found other choice for gp "+str(gpi)+": "+str(otherVar.name)+", "+str(otherVarChoiceKey)+": "+str(choiceGPlist))
                                        choiceGPlist = choiceGPlist.copy() # copy gpList so removing gpi doesn't change parent node
                                        choiceGPlist.remove(gpi)
                                        updatedGpLists[otherVarChoiceKey] = choiceGPlist
                                        if not choiceGPlist:
                                            print("removeDuplicateObs() GP "+str(gpi) + ", removing empty choice: "+str(otherVarChoiceKey)+" from var: "+str(otherVar))
                                            choicesToRemove.append(otherVarChoiceKey)
                                if choicesToRemove or updatedGpLists:
                                    otherVarCopy = self.findReplacementVar(varIndex, replacementVars)
                                    if not otherVarCopy:
                                        otherVarCopy = copy.deepcopy(otherVar)
                                        replacementVars.append((varIndex, otherVarCopy))
                                    for updatedGpListKey in updatedGpLists.keys():
                                        if updatedGpListKey in otherVar.choices:
                                            otherVarCopy.choices[updatedGpListKey] = updatedGpLists[updatedGpListKey]
                                    if otherVarCopy not in changedVars:
                                        changedVars.append(otherVarCopy)
                                    for choice in choicesToRemove:
                                        otherVarCopy.choices.pop(choice) # pop dictionary by key
                                        self.removedChoices += 1

        for varIndex, otherVarCopy in replacementVars:
            node.unassignedVars[varIndex] = otherVarCopy

        varsToRemove = []
        if changedVars:
            for v in changedVars:
                if not self.varHasChoices(v):
                    varsToRemove.append(v)
        if varsToRemove:
            for v in varsToRemove:
                print("removeDuplicateObs() removing empty var: "+str(v))
                if v in node.unassignedVars:
                    node.unassignedVars.remove(v)
                self.removedVars += 1

    def removeInfeasibleSlewChoices(self, node, var):
        maxSlewTime = 22
        changedVars = []
        satId = var.satId
        earliestSlewTick = var.tick + 2 # var.tick + 1 is obsFinish, so varTick + 2 is first slew tick
        fromAngle = self.getPointingAngleFromChoice(var.assignment[0])
        replacementVars = []
        for otherVar in node.unassignedVars:
            otherVarSat = otherVar.satId
            otherVarTick = otherVar.tick
            # otherVarPayload = otherVar.payload
            if otherVarTick - earliestSlewTick > maxSlewTime:
                break
            elif satId != otherVarSat:
                continue # ignore other var if it's from another sat or if it's tick is > maxSlewTime later than endTick
            choices = otherVar.choices
            choicesToRemove = []
            for choiceKey in choices.keys():
                # fullCmd = choiceKey
                # if isinstance(choiceKey, int):
                #     fullCmd = otherVarPayload + "."+str(choiceKey)
                toAngle = self.getPointingAngleFromChoice(choiceKey)
                if fromAngle != toAngle:
                    slewTime, slewEnergy = self.getSlewTimeAndEnergy(fromAngle, toAngle)
                    earliestStartWithSlew = earliestSlewTick + slewTime + 1 # add 1 to leave room for next cmd start
                    if otherVarTick < earliestStartWithSlew :
                        # print("  Infeasible choice: " + str(choiceKey) + ", slew time from: " + str(fromAngle) + " @ " + str(earliestSlewTick) + " to " + str(toAngle) + " @ " + str(otherVar.tick) + " = " + str(slewTime) + ", earliestStartWithSlew: "+str(earliestStartWithSlew))
                        choicesToRemove.append(choiceKey)
            if choicesToRemove:
                otherVarCopy = copy.deepcopy(otherVar)
                varIndex = node.unassignedVars.index(otherVar)
                replacementVars.append((varIndex, otherVarCopy))
                if otherVarCopy not in changedVars:
                    changedVars.append(otherVarCopy)
                for infeasibleChoice in choicesToRemove:
                    # print("removeInfeasibleSlewChoices() removing choice: "+str(infeasibleChoice))
                    otherVarCopy.choices.pop(infeasibleChoice) # pop dictionary by key
                    self.removedChoices += 1

        for varIndex, otherVarCopy in replacementVars:
            node.unassignedVars[varIndex] = otherVarCopy

        varsToRemove = []
        if changedVars:
            for v in changedVars:
                if not self.varHasChoices(v):
                    varsToRemove.append(v)
        if varsToRemove:
            for v in varsToRemove:
                # print("removeInfeasibleSlewChoices() removing empty var: "+str(v))
                node.unassignedVars.remove(v)
                self.removedVars += 1

    def findReplacementVar(self, varIndex, replacementVars):
        for pair in replacementVars:
            if pair[0] == varIndex:
                otherVarCopy = pair[1]
                return otherVarCopy
        return None

    # def removeImageLockedChoices(self, node, var):
    #     changedVars = []
    #     satId = var.satId
    #     endTick = var.tick + 2
    #     currentCmd = var.assignment[0]
    #     choiceInfo = dshieldUtil.parseChoice(currentCmd)
    #     fromAngle= choiceInfo['pointingOption']
    #     fromPayload = choiceInfo['payload']
    #     replacementVars = []
    #     for otherVar in node.unassignedVars:
    #         otherVarSat = otherVar.satId
    #         otherVarTick = otherVar.tick
    #         if otherVarTick > endTick:
    #             break
    #         elif satId != otherVarSat:
    #             continue # ignore other var if it's from another sat
    #         choices = otherVar.choices
    #         choicesToRemove = []
    #         for choiceKey in choices.keys():
    #             choiceInfo = dshieldUtil.parseChoice(choiceKey)
    #             toAngle= choiceInfo['pointingOption']
    #             toPayload = choiceInfo['payload']
    #             if fromAngle != toAngle:
    #                 print("removeImageLockedChoices() current cmd: "+ str(currentCmd)+ ", end tick: "+str(endTick)+ ", blocked cmd: " + str(choiceKey) + " @ " + str(otherVarTick))
    #                 choicesToRemove.append(choiceKey)
    #             elif fromPayload != toPayload and (var.tick + 2 == otherVarTick):
    #                 print("removeImageLockedChoices() current cmd: "+ str(currentCmd)+ ", end tick: "+str(endTick)+ ", blocked cmd: " + str(choiceKey) + " @ " + str(otherVarTick) +" payload startup: "+toPayload)
    #                 choicesToRemove.append(choiceKey)
    #         if choicesToRemove:
    #             otherVarCopy = copy.deepcopy(otherVar)
    #             varIndex = node.unassignedVars.index(otherVar)
    #             replacementVars.append((varIndex, otherVarCopy))
    #             if otherVarCopy not in changedVars:
    #                 changedVars.append(otherVarCopy)
    #             for infeasibleChoice in choicesToRemove:
    #                 print("removeImageLockedChoices() removing choice: "+str(infeasibleChoice))
    #                 otherVarCopy.choices.pop(infeasibleChoice) # pop dictionary by key
    #                 self.removedChoices += 1
    #
    #     for varIndex, otherVarCopy in replacementVars:
    #         node.unassignedVars[varIndex] = otherVarCopy
    #
    #     varsToRemove = []
    #     if changedVars:
    #         for v in changedVars:
    #             if not self.varHasChoices(v):
    #                 varsToRemove.append(v)
    #     if varsToRemove:
    #         for v in varsToRemove:
    #             print("removeImageLockedChoices() removing empty var: "+str(v))
    #             node.unassignedVars.remove(v)
    #             self.removedVars += 1
    #
    # def removeImageLockedVarsOld(self, node, var):
    #     # TODO: instead of removing var, commit to assignment if choice is still available and collect obs scores
    #     satId = var.satId
    #     tick = var.tick
    #     varsToRemove = []
    #     for otherVar in node.unassignedVars:
    #         otherVarSatId = otherVar.satId
    #         if satId != otherVarSatId:
    #             continue
    #         tickDiff = otherVar.tick - tick
    #         if tickDiff <= 2:
    #             varsToRemove.append(otherVar)
    #         elif tickDiff > 2:
    #             continue
    #     if varsToRemove:
    #         for v in varsToRemove:
    #             # print("removing locked var: "+str(v))
    #             node.unassignedVars.remove(v)
    #             self.removedVars += 1

    def removeImpossibleGapSlewChoices(self, tick, choices, gap):
        # prunes impossible choices before creating decision vars for gap
        # print("removeImpossibleGapSlewChoices() tick: "+str(tick)+", choices: "+str(choices)+", gap: "+str(gap))
        maxSlewTime = 22
        gapStart = gap['gapStart']
        gapEnd = gap['gapEnd']
        gapStartAngle = gap['startAngle']
        gapEndAngle = gap['endAngle']

        if gapStartAngle < 0:
            # print("removeImpossibleGapSlewChoices() ignoring gap with no startAngle")
            return
        prunedChoices = {}
        observationStart = tick
        observationEnd= tick + 2 # three ticks, including first tick and last tick
        for choice in choices.keys():
            choiceAngle = self.getPointingAngleFromChoice(choice)
            initialSlewDur, initialSlewEnergy = self.getSlewTimeAndEnergy(gapStartAngle, choiceAngle)
            finalSlewDur, slewEnergy = self.getSlewTimeAndEnergy(choiceAngle, gapEndAngle)
            earliestObsStartAfterInitialSlew = gapStart + initialSlewDur
            latestObsEndBeforeFinalSlew = gapEnd - finalSlewDur

            if observationStart < earliestObsStartAfterInitialSlew: # verify there's enough time to slew from gap start angle to choice angle
                pass
                # print("  Infeasible gap choice: " + str(choice) + ", slew time gap start angle: " + str(gapStartAngle) + " @ " + str(gapStart-1) + " to choice angle: " + str(choiceAngle) + " @ " + str(observationStart) + " = " + str(initialSlewDur) + ", earliestObsStartAfterInitialSlew: "+str(earliestObsStartAfterInitialSlew))
            elif observationEnd > latestObsEndBeforeFinalSlew:   # verify there's enough time to slew from choice angle to gap end angle
                pass
                # print("  Infeasible gap choice: " + str(choice) + ", choice angle: " + str(choiceAngle) + " @ " + str(observationEnd) + " to gap end angle: " + str(gapEndAngle) + " @ " + str(gapEnd + 1) + " = " + str(finalSlewDur) + ", latestObsEndBeforeFinalSlew: "+str(latestObsEndBeforeFinalSlew))
            else:
                choiceDict = {choice: choices[choice]}
                prunedChoices.update(choiceDict)
        # if len(choices) != len(prunedChoices):
        #     print(" pruned gap choices: in: "+str(choices)+", out: "+str(prunedChoices))

        return prunedChoices
#===================================
#     Heuristics

    def sortTpChoices(self, var):
        if self.valSelectorHeuristic == "gpRankedChoice":
            return self.sortTpChoicesExhaustiveBallot(var, 5)
        tick = var.tick
        choices = var.choices
        # cmdChoices = (cmd, reward, obsCount, gpList)
        cmdRewards = [] # (cmd, reward, obsCount)
        for cmd in choices.keys():
            gpList = choices[cmd]# [:-1] # strip off score
            reward = self.getCmdReward(var.tick, cmd, gpList, self.valSelectorHeuristic)
            if reward < 0:
                print("sortTpChoices() ERROR! negative reward "+str(reward)+ " for var: "+str(var)+", cmd: "+str(cmd)+", gpList: "+str(gpList))
                y = 7/0
            # fullCmd = cmd
            # if isinstance(cmd, int):
            #     fullCmd = var.payload+"."+str(cmd)
            cmdRewards.append((cmd, gpList, reward, self.getObsCount(cmd)))
        # sort first by decreasing reward, then by increasing observation count
        if cmdRewards:
            cmdRewards = sorted(cmdRewards, key=lambda x: (-x[2], x[3]))
        return cmdRewards

    def sortTpChoicesExhaustiveBallot(self, var, beamWidth):
        # Preferential Voting Block algorithm
        if self.voteLogging:
            print("sortTpChoicesExhaustiveBallot() var: "+str(var))

        # initialize ballot
        satId = var.satId
        tick = var.tick
        candidates = var.choices

        candidatesCount = len(candidates)
        seats = min(beamWidth, candidatesCount)
        electedCandidates = []
        voters = set()
        for candidate in candidates.keys():
            candidateVoters = set(candidates[candidate])
            voters.update(candidateVoters)
        votesCast = {}
        for voter in voters:
            votesCast[voter] = 0   # votes cast by voter
        unelectedCandidates = list(candidates.keys())

        #convert from angle to err code
        candidateChoices = {}
        for unelected in unelectedCandidates:
            choiceInfo = dshieldUtil.parseChoice(unelected)
            payload = choiceInfo['payload'] # TODO: handle multiple obs
            errorCode = choiceInfo['errorCode']
            key = payload +"."+str(errorCode)
            if key not in candidateChoices:
                candidateChoices[key] = []
            candidateChoices[key].append(unelected)

        electedChoices = []
        results = {}
        allSeatsFilled = False
        # start voting
        for votingRound in range(seats):
            if allSeatsFilled:
                break
            if self.voteLogging:
                print("\nstarting round "+str(votingRound))
            votes = {}
            # unelectedCandidates = list(unelectedChoices.keys())
            for candidate in candidateChoices:
                votes[candidate] = 0   # votes for candidate
            # each voter votes for their favorite candidate (or abstains)
            for candidate in candidateChoices:
                if allSeatsFilled:
                    break
                if self.voteLogging:
                    print("\ncollecting votes for candidate: "+str(candidate) +", slate: "+str(candidateChoices)+", all Votes: "+str(votes))
                # choiceInfo = dshieldUtil.parseChoice(candidate)
                # payload = choiceInfo['payload']
                # errorCode = choiceInfo['errorCode'] # TODO: handle multiple obs
                # candidateChoice = payload +"."+str(errorCode)
                for voter in voters:
                    if votesCast[voter] < seats:
                        if self.isFavoriteCandidate(satId, tick, voter, candidate, electedChoices):
                            if self.voteLogging:
                                print("vote cast "+str(voter)+": "+str(candidate))
                            votes[candidate] += 1
                            votesCast[voter] += 1
                # tally the vote and elect winners
                if votes[candidate] > len(voters)/2:
                    if self.voteLogging:
                        print("\nround "+str(votingRound)+", elected: "+str(candidate) +", votes: "+str(votes[candidate]))
                    electedChoices.append(candidate)
                    for cmd in candidateChoices[candidate]:
                        electedCandidates.append(cmd)
                        # unelectedCandidates.pop(candidate)
                        if len(electedCandidates) >= seats:
                            allSeatsFilled = True
                            break

            results[votingRound] = votes
            if self.voteLogging:
                print("round "+str(votingRound)+" results: "+str(votes)+"\n")
                print("\nAll seats filled ("+str(len(electedCandidates))+"): "+str(electedCandidates))

        result = []
        for elected in electedCandidates:
            result.append((elected, candidates[elected], 1))
            # result[elected] = candidates[elected]
        if self.voteLogging:
            print("sortTpChoices() result: "+str(result)+"\n")
        # Return format: (cmd, gpList, reward, self.getObsCount(cmd))
        # result is sorted first by decreasing reward, then by increasing observation count
        return result

    # def isFavoriteCandidate(self, satId, tick, voter, candidate, electedChoices):
    #     # voter = <gpid, # votes cast by gpid>
    #     # Each voter must vote for it's choices in order, excluding elected candidates
    #     # If voter's first choice is already elected then it must vote for second (first unelected choice)
    #     # If voter's first unelected choice is not in candidates, then return None
    #
    #     # minErr and best ignore satId and tick, but can only elect candidates that match satId and tick
    #     # exit after round where no candidates are elected
    #     gp = self.getGP(voter)
    #     gpChoices = gp.errorTableChoices
    #     modelErr = self.getGpModelErr(gp, tick)
    #     voterChoices = []
    #     for choice in gpChoices:
    #         if choice['obs'] == 1:
    #             choiceSatId = choice['satId']
    #             choiceTick  = list(choice.keys())[1] # tick is second key of choice
    #             err = choice['err']
    #             if err < modelErr:
    #                 tickChoice = choice[choiceTick]
    #                 payloads = list(tickChoice.keys())
    #                 choicePayload = payloads[0]
    #                 choiceCode = tickChoice[choicePayload]
    #                 choiceCandidate = choicePayload + "."+str(choiceCode)
    #                 if len(payloads) == 2:
    #                     choiceCandidate += "."+str(payloads[1])+"."+str(choiceCode)
    #                 choiceDict = {"satId": choiceSatId, "tick": choiceTick, "candidate": choiceCandidate, "err": err}
    #                 voterChoices.append(choiceDict)
    #             else:
    #                 self.skippedCandidates += 1
    #
    #
    #     if self.voteLogging:
    #         print("\nisFavorite() v: "+str(voter)+", c: "+str(candidate) +", elected: "+str(electedChoices))
    #         print("voterChoices ("+str(len(voterChoices))+")")
    #         for c in voterChoices:
    #             print(str(c))
    #         print("gpChoices ("+str(len(gpChoices))+")")
    #         for c in gpChoices:
    #             print(str(c))
    #     minErr = None
    #     for choice in voterChoices:
    #         choiceCandidate = choice["candidate"]
    #         choiceErr = choice["err"]
    #         choiceSatId = choice["satId"]
    #         choiceTick = choice["tick"]
    #         if choiceCandidate not in electedChoices:
    #             if not minErr:
    #                 minErr = choiceErr
    #             elif minErr < choiceErr:
    #                 break
    #             if choiceCandidate == candidate and choiceSatId == satId and choiceTick == tick:
    #                 # print("voter: "+str(voter) + ", choices: "+str(unelectedChoices)+ ", testCandidate: "+str(testCandidate)+", choiceCandidate: "+str(choiceCandidate) +", vote")
    #                 return True
    #             else: # return false if first unelected choice is not candidate
    #                 # print("voter: "+str(voter) + ", choices: "+str(unelectedChoices)+ ", choiceCandidate: "+str(choiceCandidate) +", no vote")
    #                 return False

    def getCmdReward(self, tick, cmd, gpList, heuristic):
            reward = 0
            if heuristic == "maxGpCount":
                reward = self.getRewardForGpCountHeuristic(gpList)
            # elif heuristic == "maxGpRankedChoice":
            #     reward = self.getRewardForRankedChoiceHeuristic(tick, cmd, gpList)
            # elif heuristic == "minGpChoice":
            #     reward = self.getRewardForGpChoiceHeuristic(cmd, gpList)
            elif heuristic == "maxErrReduction":
                reward = self.getRewardForErrReductionHeuristic(tick, cmd, gpList)
            return reward

    def getRewardForGpCountHeuristic(self, gpList):
        return len(gpList)

    # def getRewardForGpChoiceHeuristic(self, var, cmd, gpList):
    #     # return sum of gpErr, for all gp in list
    #     totalErr = 0
    #     for gpi in gpList:
    #         gp = self.getGP(gpi)
    #         gpErr = self.getGpChoiceErr(gp, var.tick, cmd)
    #         totalErr += gpErr
    #     reward = dshieldUtil.getNormalizedScore(gpErr)
    #     return reward

    def getRewardForErrReductionHeuristic(self, tick, cmd, gpList):
        # called by getCmdReward()
        # return sum of gpModelErr - gpMeasurementErr, for all gp in list
        totalErrReduction = 0
        for gpi in gpList:
            # if isinstance(cmd, int):
            #     cmd = var.payload+"."+str(cmd)
            errReductionForGp = self.getGpReward(gpi, tick, cmd)
            totalErrReduction += errReductionForGp
        return totalErrReduction

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

    def getGpChoiceRank(self, gp, tp, choice):
        # returns position of choice in gp's preference list (errorTable sortd by increasing err)
        # ignores tp and satId because error is not a function of either
        #TODO: handle ties (currently enforces total order)
        choiceInfo = dshieldUtil.parseChoice(choice)
        payload = choiceInfo["payload"]
        pointingOption = choiceInfo["pointingOption"]
        errorTableCode = choiceInfo["errorCode"]
        errorTableChoices = gp.errorTableChoices
        if not errorTableChoices:
            print("getGpChoiceRank() ERROR! errorTableNotFound! for tp: " + str(tp) + ", payload: " + str(payload) + ", pointingOpt: " + str(pointingOption))
        rank = 0
        # choiceCount = len(errorTableChoices)
        priorErr = 0
        for tableChoice in errorTableChoices:  # assumes choices are sorted in increasing error order (done in preprocessor: getErrTableChoices())
            rank += 1
            # TODO: handle multiple obs at different times
            if tp in tableChoice and tableChoice[tp]:
                choicePayload = list(tableChoice[tp].keys())[0]
                choiceCode = tableChoice[tp][choicePayload]
                if payload == choicePayload and errorTableCode == choiceCode:
                    # print("getGpChoiceRank() gp: "+str(gp.id)+", tp: "+str(tp)+", choice: "+str(choice)+", rank: "+str(rank))
                    return rank

        print("getGpChoiceRank() ERROR! choice "+str(choice) +" not found in error table for GP "+str(gp.id)+", tp: "+str(tp))

    def getGpModelErr(self, gp, tick):
        modelTime = self.getModelTime(tick) # convert 6 hour horizon into 3 hour time indices used by soil model
        modelErrors = gp.initialModelError
        for time, err in modelErrors:
            if time == modelTime:
                return err

    def getFinalGpModelErr(self, gpi):
        gp = self.getGP(gpi)
        return gp.finalModelError

    def getGpChoiceScore(self, gp, tp, choice):
        # return choiceScore
        choiceInfo = dshieldUtil.parseChoice(choice)
        payload = choiceInfo["payload"]
        errorTableCode = choiceInfo["errorCode"]
        payload2 = None
        payload2ErrCode = None
        if 'payload2' in choiceInfo:
            payload2 = choiceInfo["payload2"]
            payload2ErrCode = choiceInfo['errorCode2']
        gpErrorTableChoices = gp.errorTableChoices
        if not gpErrorTableChoices:
            print("getGpChoiceScore() ERROR! errorTableNotFound! for tp: " + str(tp) + ", payload: " + str(payload)+", choice: "+str(choice))
        for gpErrorTableChoice in gpErrorTableChoices:  # assumes choices are sorted in increasing error order
            # get gp error table choices for tp
            if tp in gpErrorTableChoice and gpErrorTableChoice[tp]:
                gpTpChoice = gpErrorTableChoice[tp]  # gpTpChoice Examples:  {'L': 3, 'P': 3} or ('L': 3, 'L': 3}
                choicePayload = list(gpTpChoice.keys())[0] # first key in gpErrorTableChoice[tp]
                choiceCode = gpTpChoice[choicePayload]
                if payload == choicePayload and errorTableCode == choiceCode:
                    tableChoiceScore = gpErrorTableChoice["score"]
                    obsCount = gpErrorTableChoice['obs']
                    if not payload2 and obsCount == 1:
                        return tableChoiceScore
                    elif payload2 and obsCount > 1: # multiple simultaneous obs (2 instruments at same time and same angle)
                        # TODO: filter out case where second obs is a second tp
                        if payload2 != payload: # TODO: Fix HACK! Assumes simultaneous multiObs, when must be different payloads
                            if payload2 in gpTpChoice: # TODO: is this a sufficient test?
                                choiceCode2 = gpTpChoice[payload2]
                                if payload2ErrCode == choiceCode2:
                                   return tableChoiceScore

        print("getGpChoiceScore() ERROR! choice "+str(choice) +" not found in error table for GP "+str(gp.id)+", tp: "+str(tp))
        return None

#================================
#   Planner callbacks

    def nodeSorter(self):
        searchStrategy = self.nodeSorterHeuristic
        winners = []
        if self.planner.openNodes:
            if searchStrategy == "beam":
                winners = self.sortNodesByPlanReward()
            elif searchStrategy == "dfs":
                # return open nodes in reverse order
                winners.extend(self.planner.openNodes)
                winners.reverse()
                # bestIndex = -2 if len(self.planner.openNodes) > 1 else -1
                # winners.append(self.planner.openNodes[bestIndex]) # winner is a node (not node id)
                #don't select first child from root if it doesn't collect any data
                # if self.planner.isRootChild(winner) and winner.choice == 0:
                #     print("nodeSorter() no plan! winners: "+str(winner))
                #     winners.append(self.planner.openNodes[0])
        else:
            print("nodeSorter() No open nodes!")
        # print("nodeSorter() exit winners: "+str(winners))
        return winners

    def varSelector(self, node):
        if node.unassignedVars:
            bestVar = node.unassignedVars[0]
        else:
            print("varSelector() No unassigned vars!")
        # print("varSelector() result: " + str(bestVar.name))
        return bestVar

    def valSorter(self, node, var):
        result = []
        sortedChoices  = self.sortTpChoices(var) # returns tuples: (cmd, gpList, reward)
        # return sorted lists with non-empty gpLists (necessary check?)
        for choice in sortedChoices:
            cmd = choice[0]
            gpList = choice[1]
            reward = choice[2]
            if gpList:
                result.append((cmd, gpList, reward))
            else:
                print("empty choice: "+str(cmd))
        if result:
            return result
        else:
            print("valSorter() ERROR! no choices!")


    def sortNodesByPlanReward(self):
        result = sorted(self.planner.openNodes, key=lambda x: x.planReward, reverse=True)
        return result

#===========================
#   Multiple Observations

    def appendMultiObs(self, choices):
        print("appendMultiObs() choices: "+str(choices))
        obsPairs = []
        for choiceKey1 in choices.keys():
            instrument1, angle1 = choiceKey1.split(".")
            if instrument1 == 'L':
                angle1 = int(angle1)
                for choiceKey2 in choices.keys():
                    instrument2, angle2 = choiceKey2.split(".")
                    if instrument2 == 'P':
                        angle2 = int(angle2)
                        if angle1 == angle2:
                            obsPairs.append(('L.'+str(angle1),'P.'+str(angle2)))
        print("appendMultiObs() obsPairs: "+str(obsPairs))
        newChoices = {}
        for o1, o2 in obsPairs:
            gpSet1 = set(choices[o1])
            gpSet2 = set(choices[o2])
            gpList = sorted(list(gpSet1.intersection(gpSet2)))
            err = self.getMultiObsErrScore(o1, o2)
            score = dshieldUtil.getNormalizedScore(err) * len(gpList)
            gpList.append(score)
            key = o1+"."+o2
            newChoices[key] = gpList
        print("appendMultiObs() newChoices: "+str(newChoices))
        return newChoices

    def getMultiObsErrScore(self, o1, o2):
        # TODO: handle biome-dependent errors
        print("getMultiObsErrScore() o1: "+o1+", o2: "+o2)
        i1, a1 = o1.split(".")
        i2, a2 = o2.split(".")
        c1 = dshieldUtil.getErrorTableCode(int(a1))
        c2 = dshieldUtil.getErrorTableCode(int(a2))
        row = (c1,c2)
        error = dshieldUtil.getError(row, self.errorTable[7])
        print ("row: "+str(row)+", err: "+str(error))
        return error

    def getGpReward(self, gpi, tick, cmd):
        # called by filterNegativeRewards()
        gp = self.getGP(gpi)
        # modelTime = self.getModelTime(tick)
        priorErr = self.getGpModelErr(gp, tick)
        errorTableType = dshieldUtil.getErrorTableTypeFromBiomeType(gp.type)
        if errorTableType not in self.errorTable:
            print("getGpReward() ERROR! unknown biome type for gp: " + str(gpi))
            errorTableType = 7
        cmdErr = dshieldUtil.getCmdErr(cmd, self.errorTable[errorTableType])
        reward = priorErr - cmdErr
        return reward

#================================
#   Execution

    def executePlan(self, plan):
        gpErrResultsCount = len(list(self.gpErrResults.keys()))
        print("ExecutingPlan: "+str(len(plan))+ " steps, errResultsCount: "+str(gpErrResultsCount))
        gpCount = 0
        stepCount = 0
        totalErr = 0
        for node in plan:
            stepCount += 1
            var = node.var
            if var:
                satId = var.satId
                cmd = var.assignment[0]
                gpList = var.assignment[1]
                gpCount += len(gpList)
                print("node: "+str(node.id) + " sat: "+str(satId)+", cmd: " + cmd + ", gpList: " + str(gpList))
                for gpId in gpList:
                    gp = self.getGP(gpId)
                    errorTableType = dshieldUtil.getErrorTableTypeFromBiomeType(gp.type)
                    if errorTableType not in self.errorTable:
                        print("executePlan() ERROR! unknown biome type for gp: " + str(gp))
                        errorTableType = 7
                    cmdErr = dshieldUtil.getCmdErr(cmd, self.errorTable[errorTableType])
                    gp.measurementError = cmdErr
                    gp.finalModelError = cmdErr
                    totalErr += cmdErr
                    if gpId in self.gpErrResults:
                        print("executePlan() ERROR! duplicate GP overwriting gpErrResult! new: "+str(cmdErr)+" old: "+str(self.gpErrResults[gpId]))
                        if cmdErr <= self.gpErrResults[gpId]:
                            self.gpErrResults[gpId] = cmdErr
                        else:
                            print("executePlan() ERROR! ignoring duplicate GP which is worse new: "+str(cmdErr)+" old: "+str(self.gpErrResults[gpId]))
                    else:
                        self.gpErrResults[gpId] = cmdErr

        avgErr = round(totalErr /gpCount, 5)
        gpErrResultsCount = len(list(self.gpErrResults.keys()))
        print("\n\n*******\n\nExecutePlan() result planSteps: "+str(stepCount)+", gpCount: "+str(gpCount)+", totalErr: "+str(totalErr)+", avgErr: "+str(avgErr)+", gpErrResults count: "+str(gpErrResultsCount))

#==================
#      Analysis and stats

    def analyzeResults(self):
        # TODO: write this to file
        timestamp = datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S")
        print("\n\nSolution Summary "+timestamp)
        print("Global heuristic: " + str(self.nodeSorterHeuristic)+", Local Heuristic: "+self.valSelectorHeuristic+", Beam width: "+ str(self.planner.nodeBeamWidth) +", fixedPointing: "+str(self.fixedPointingOption))
        print("Max Tick: "+str(self.maxTick)+", multipleRoots: "+str(self.multipleRootNodes)+", useSortedGP: "+str(self.useSortedGP)+", sortedGPpct: "+str(self.sortedGPpct))
        print("Max Battery Charge: "+str(self.maxBatteryCharge)+" W-H, solarPowerIn: "+str(self.powerIn)+ " W")
        print("LowestEnergy: "+str(self.lowestEnergy))
        print("InitialPlan node count: "+str(self.initialPlanNodeCount)+", finalPlan node count: "+str(self.planner.nextNodeId))
        if self.valSelectorHeuristic == "gpRankedChoice":
            print("skipped candidates: "+str(self.skippedCandidates))
        totalPlanSize = 0
        totalObservedGp = 0
        for satId in self.solutionSummary.keys():
            # satSummary = {'planSize': satNodeCount, 'observedGP': satObservedGpCount, 'satGpPerImage': satGpPerImage, 'unique Observed GP': satUniqueObservedGpCount}
            satSummary = self.solutionSummary[satId]
            totalPlanSize += satSummary['planSize']
            totalObservedGp += satSummary['observedGP']
            print("s"+str(satId)+" summary: "+str(satSummary))
        horizonGpCount = len(self.horizonGPs)
        print("\nSwarm totals: planSize: "+str(totalPlanSize)+", observedGP: "+str(totalObservedGp)+", horizonGP: "+str(horizonGpCount))

        gpErrResultCount = len(self.gpErrResults.keys())
        totalError = 0
        print("analyzeResults() planSize: "+str(len(self.plan)) +", gpErrCount: "+str(gpErrResultCount)+", observedGp: "+str(len(self.observedGPs)))
        # gpErrResults populated by ExecutePlan()
        for gp in self.gpErrResults.keys():
            gp = self.getGP(gp)
            err = gp.measurementError
            totalError += err
        avgErr = dshieldUtil.roundIt(totalError/gpErrResultCount)
        avgHorizonErrReduction = dshieldUtil.roundIt(self.initialHorizonGpErrAvg *  - avgErr)
        errInfo = self.getInitialObservedGpErrAvg()
        self.initialObservedGpErrAvg = errInfo['avgErr']
        self.initialObservedGpErrTotal = errInfo['totalErr']
        self.initialObservedGpErrCount = errInfo['gpCount']
        avgObservedErrReductionFromPlan = dshieldUtil.roundIt(self.initialObservedGpErrAvg - avgErr)
        print("analyzeResults() totalErr: "+str(totalError)+", execution GP Count: "+str(gpErrResultCount)+", err/gp avg: "+str(avgErr))
        print("analyzeResults() initial Horizon GP err avg: "+str(self.initialHorizonGpErrAvg)+", avgErrReduction: "+str(avgHorizonErrReduction))
        finalErrInfo = self.getFinalObservedGpErrAvg()
        finalObservedGpErrAvg = finalErrInfo['avgErr']
        finalObservedGpErrTotal = finalErrInfo['totalErr']
        finalObservedGpErrCount = finalErrInfo['gpCount']
        avgObservedErrReductionFromState = dshieldUtil.roundIt(self.initialObservedGpErrAvg - finalObservedGpErrAvg)
        totalErrReduction = self.initialObservedGpErrTotal - finalObservedGpErrTotal
        avgObservedErrReductionFromTotals = dshieldUtil.roundIt(totalErrReduction/finalObservedGpErrCount)
        totalInitialHorizonError = self.initialHorizonGpErrAvg *  horizonGpCount
        avgHorizonErrReduction = (totalInitialHorizonError- totalErrReduction) / horizonGpCount
        print("analyzeResults() initial Observed GP err summary: total: "+str(self.initialObservedGpErrTotal)+", avg: "+str(self.initialObservedGpErrAvg)+", count: "+str(self.initialObservedGpErrCount))
        print("analyzeResults() final Observed GP err summary: total: "+str(finalObservedGpErrTotal)+", avg: "+str(finalObservedGpErrAvg)+", count: "+str(finalObservedGpErrCount))
        print("analyzeResults() avgErrReduction: from plan cmds: "+str(avgObservedErrReductionFromPlan)+", from last node state: "+str(avgObservedErrReductionFromState)+", from totals: "+str(avgObservedErrReductionFromTotals))
        print("analyzeResults() avg horizon err reduction per horizon gp: "+str(avgHorizonErrReduction))
        print("analyzeResults() total err reduction (objective score): "+str(totalErrReduction))
        print("analyzeResults() done")

    def comparePlans(self, filename1, filename2):
        filepath1 = self.dataPath + filename1
        filepath2 = self.dataPath + filename2
        gpList1 = set()
        gpList2 = set()
        if os.path.exists(filepath1):
            print("reading prior observations from file: "+filename1)
            f = open(filepath1, "r")
            readingGP = False
            for line in f:
                line = line.strip()
                if line.startswith("# Observed GP"):
                    readingGP = True
                elif not line.startswith("#"):
                    if readingGP:
                        terms = line.split(",")
                        count = len(terms)
                        print("parsed "+str(count) +" terms")
                        for term in terms:
                            gpList1.add(int(term))
            f.close()
        if os.path.exists(filepath2):
            print("reading prior observations from file: "+filename2)
            f = open(filepath2, "r")
            readingGP = False
            for line in f:
                line = line.strip()
                if line.startswith("# Observed GP"):
                    readingGP = True
                elif not line.startswith("#"):
                    if readingGP:
                        terms = line.split(",")
                        count = len(terms)
                        print("parsed "+str(count) +" terms")
                        for term in terms:
                            gpList2.add(int(term))
            f.close()
        diff1 = sorted(list(gpList1-gpList2))
        diff2 = sorted(list(gpList2-gpList1))
        if diff1:
            print("plan diff1 ("+str(len(diff1))+"): "+str(diff1))
        if diff2:
            print("plan diff2 ("+str(len(diff2))+"): "+str(diff2))
        if not diff1 and not diff2:
            print("plans are identical")

    def statsInit(self):
        self.stats["timerStart"] = time.time()
        self.stats["startTime"] = time.localtime()
        self.stats["startTimestamp"] = time.strftime("%H:%M:%S", self.stats["startTime"]) # "%b.%-d.%y.%H:%M:%S"
        self.stats["planId"] = 1
        self.stats["hStart"] = self.horizonStart
        self.stats["hDur"] = self.horizonDur
        # self.stats["removeOldObs"] = self.removeOldObs
        # self.stats["removeNewObs"] = self.removeNewObs
        self.stats["imageLock"] = self.imageLock
        self.stats["valSelectorHeuristic"] = self.valSelectorHeuristic
        self.stats["blockoutSlewTimes"] = self.blockoutSlewTimes
        print("Start time: "+self.stats["startTimestamp"])

    def updateStats(self, successNode):
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
        # self.stats["planCount"] = self.planCount
        horizonGpCount  = len(self.horizonGPs)
        observedGpCount = len(self.observedGPs)
        missedGpCount = horizonGpCount - observedGpCount
        self.stats["horizonGp"] = horizonGpCount
        self.stats["observedGp"] = observedGpCount
        self.stats["missedGP"] = missedGpCount
        self.stats["coveredPct"] = round(observedGpCount/horizonGpCount *100, 3)
        self.stats["missedPct"] = round(missedGpCount/horizonGpCount *100, 3)
        depth = None
        energy = None
        imageCount = len(self.plan)
        self.stats['imageCount'] = imageCount
        # if "depth" in successNode.state:
        #     depth = successNode.state["depth"]
        # self.stats["imageCount"] = len(self.plan) #depth
        # if "energy" in successNode.state:
        #     energy = successNode.state["energy"]
        # if depth and energy:
        #     self.stats["gp/image"] = round(observedGpCount / imageCount, 3)
            # self.stats["energy"] = round(energy, 3)
            # self.stats["energy/gp"] = round(energy/observedGpCount, 3)
            # self.stats["energy/image"] = round(energy/imageCount, 3)

        # errs = self.collectObservationErrors(self.observedGPs)
        # self.stats.update(errs)
        self.stats.update()

    def printStats(self):
        print("\nNodeCount: " + str(self.stats["imageCount"]))
        print("Times: " + self.stats["startTimestamp"] + " - "+self.stats["endTimestamp"] + ", elapsed: " + self.stats["elapsed"])
        print("rowRewards: "+str(round(self.rowRewards, 5)))

    def printGPsummary(self):
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
                    self.singleAccessGP.append(gp)
                totalAccessTimeCount += gpAccessTimeCount
                if gpAccessTimeCount > maxAccessTimeCount:
                    maxAccessTimeCount = gpAccessTimeCount
                    self.maxAccessGP = gp
            else:
                noAccessCount += 1
            if len(gp.rainHours) > 0 and i < 25:
                print("GP "+str(gp))
                i += 1

        if gpCount:
            avgAccessTimeCount = totalAccessTimeCount / gpCount
            print("GP summary:  count: "+str(gpCount)+", avgAccessTimeCount/GP: "+str(avgAccessTimeCount)+", max: "+str(maxAccessTimeCount)+", singleAccessCount: "+str(singleAccessCount)+", noAccessCount: "+str(noAccessCount))
            print("maxAccessGP: "+str(self.maxAccessGP))
            i = 0
            for gpi in self.singleAccessGP:
                print("singleAccessGP: "+str(gpi))
                i += 1
                if i > 10:
                    break
        else:
            print("printGPsummary() ERROR! no GP found")
            exit(9)

    def combinePlans(self):
        # Post-processing
        # second planning round (rainOnly)
        # self.initialPlan = self.initialPlan[1:]
        self.plan = self.plan[1:] # strip off first node because it has no vars
        print("combining plans! initial plan size: "+str(self.initialPlanStepCount)+", new plan size: "+str(len(self.plan)))
        finalPlanStepObsGp = self.collectObservedGP(self.successNode) #.state['observedGp']
        self.plan.extend(self.initialPlan)
        self.plan.sort(key=lambda n: n.var.tick) # TODO: add second sort key = satId
        sortedFinalPlanStep = self.plan[-1] # find last step after sorting the combined plan
        # sortedFinalPlanStepObsGp = self.collectObservedGP(sortedFinalPlanStep) #.state['observedGp']
        duplicateObservedGpFromCombinedInitialAndGapPlans = sorted(list(set(finalPlanStepObsGp) & set(self.initialPlanObservedGPs)))
        combinedObservedGp = sorted(list(set(finalPlanStepObsGp).union(set(self.initialPlanObservedGPs))))
        self.observedGPs = combinedObservedGp
        self.successNode = sortedFinalPlanStep
        # self.successNode.state['observedGp'] = self.observedGPs

        initialHorizonGPs = len(self.initialPlanHorizonGPs)
        gapHorizonGPs = len(self.horizonGPs)
        totalHorizonGPs = initialHorizonGPs + gapHorizonGPs
        gpInBothHorizons = self.initialPlanHorizonGPs & self.horizonGPs
        self.horizonGPs.update(self.initialPlanHorizonGPs)

        # initialObservedGPs = len(self.initialPlanObservedGPs)
        # gapObservedGPs = len(self.observedGPs)
        # totalObservedGPs = initialObservedGPs + gapObservedGPs
        # self.observedGPs.extend(self.initialPlanObservedGPs)
        print("combinePlans() combined plan size: "+str(len(self.plan)))
        print("combinePlans() initial horizon GPS: "+ str(initialHorizonGPs)+", gap horizon GPs: "+str(gapHorizonGPs)+", total: "+ str(totalHorizonGPs) +", total2: "+ str(len(self.horizonGPs)))
        print("combinePlans() duplicate horizon GP count: "+str(len(gpInBothHorizons)))
        # print("combinePlans() initial observed GPS: "+ str(initialObservedGPs)+", gap observed GPs: "+str(gapObservedGPs)+", total: "+ str(totalObservedGPs) +", total2: "+ str(len(self.observedGPs)))
        if gpInBothHorizons:
            print("\ncombinePlans() duplicate GP count: "+str(len(gpInBothHorizons))) #+"\n"+str(sorted(gpInBothHorizons)))

        # finalSatPlans = self.successNode.state['satPlans']
        # self.successNode.state['satPlans'] = finalSatPlans
        # self.successNode.state['satPlans'][satId] = finalSatPlan
        # self.observedGPs = self.successNode.state['observedGp']
        # update energy after final gap if necessary
        # for satId in self.satList:
        #     finalSatPlan = finalSatPlans[satId]
        #     finalPlanStep = finalSatPlan[-1]
        #     if 'energy' not in finalPlanStep:
        #         finalNodeId = finalPlanStep['node']
        #         finalCmd  = finalPlanStep['cmd']
        #         if finalNodeId != self.successNode.id:
        #             print("\n\n**** solveIt() ERROR! finalNodeId: "+str(finalNodeId) +" != successNodeId: "+str(self.successNode.id) + " ******")
        #         previousSteps = self.collectPreviousPlanStepsSinceEnergyUpdate(finalNodeId, finalSatPlan)
        #         self.updateEnergyStates(self.successNode, finalCmd, previousSteps)


    # def collectObservationErrors(self, observedGp):
    #     # deprecated
    #     observedGpCount = len(observedGp)
    #     print("collectObservationErrors() observedGp: "+str(observedGpCount))
    #     totalErrHorizon = 0
    #     totalErrObserved = 0
    #     avgErr = 0
    #     maxErr = 0
    #     allErrs = []
    #     totalHorizonGpChoices = 0
    #     for gpId in self.horizonGPs:
    #         gp = self.getGP(gpId)
    #         gpErr = gp.measurementError
    #         allErrs.append(gpErr)
    #         totalErrHorizon += gpErr
    #         if gpId in observedGp:
    #             totalErrObserved += gpErr
    #         if gpErr > maxErr:
    #             maxErr = gpErr
    #         gpChoices = gp.errorTableChoices
    #         totalHorizonGpChoices += len(gpChoices)
    #
    #     horizonGpCount = len(self.horizonGPs)
    #     avgErrHorizon = totalErrHorizon/horizonGpCount
    #     avgErrObserved = totalErrObserved/observedGpCount
    #     avgScoreHorizon = dshieldUtil.getNormalizedScore(avgErrHorizon)
    #     avgScoreObserved = dshieldUtil.getNormalizedScore(avgErrObserved)
    #     avgGpChoices = totalHorizonGpChoices / horizonGpCount
    #     result = {"horizonGpCount": horizonGpCount, "avgChoices/gp": avgGpChoices, "totalErrHorizon": round(totalErrHorizon,3), "totalErrObserved": round(totalErrObserved,3), "avgErr/horizonGP": round(avgErrHorizon,3),"avgErr/observedGP": round(avgErrObserved,3), "avgScore/horizonGP": round(avgScoreHorizon,3),"avgScore/observedGP": round(avgScoreObserved,3), "maxErr": round(maxErr,3)}
    #     print("\ncollectObservationErrors() result: "+str(result))
    #     return result


    def collectPlan(self, node, depthLimit = None):
        result = list()
        result.append(node)
        parent = self.planner.getNode(node.parent)
        while parent and (not depthLimit or len(result) < depthLimit):
            node = parent
            result.append(node)
            parent = self.planner.getNode(node.parent)
        result.reverse()
        return result

#================================
    # file reading and writing
    def readHorizonGpFile(self):
        filename = self.getHorizonGpFilenamePrefix(self.horizonId)+".gp.txt"
        filepath = self.dataPath + "preprocessing/"+filename
        lineNumber = 0
        duplicateGP = 0
        print("readHorizonGpFile() file: "+str(filepath))
        if os.path.exists(filepath):
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
                    # if self.fixedPointingOption:
                    #     combinedErrorTable = self.filterForFixedPointingOption(gp)
                    newChoices = []
                    # TODO: make sure fixedPointing works for multiple horizons
                    #      the following condition is never True (gp.id in self.gpDict) at least in horizon 1
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
                            # if self.fixedPointingOption:
                            #     combinedErrorTable = self.filterForFixedPointingOption(combinedErrorTable)
                            gp.errorTableChoices = combinedErrorTable
                    self.gpDict[gp.id] = gp
                    lineNumber += 1
                    if lineNumber % 10000 == 0:
                        print("  line: "+str(lineNumber))
            print("GP count: "+str(lineNumber) + ", duplicate GP count: "+str(duplicateGP))
        else:
            print("readHorizonGpFile() ERROR! file not found: "+filepath)


    def filterAccessTimesForFixedPointingOption(self, choices):
        # print("choices: "+str(choices))
        filteredChoices = {}
        for choice in choices:
            choiceInfo = dshieldUtil.parseChoice(choice)
            if choiceInfo['pointingOption'] == 42:
                filteredChoices[choice] = choices[choice]
        # print("filtered choices: "+str(filteredChoices))
        return filteredChoices


    def readSlewTable(self):
        filename = self.dataPath + "preprocessing/slewTable.txt"
        print("Reading Slew Table: "+str(filename))
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

    def writePlanToFile(self, successNode, satId, timestamp):
        # uses self.plan
        # NOTE: collects gap info and inserts observation start/end tokens as side effect

        totalObsTicks = 0
        totalIdleTicks = 0
        totalSlewTicks = 0
        instrumentStates = {"L": None, "P": None}

        satName = "s"+str(satId)
        # satPlan = successNode.state['satPlans'][satId]
        filename = self.getHorizonFilenamePrefix(satId, self.horizonId) +"."+timestamp+".plan.txt"
        filepath = self.dataPath + self.experimentRun+"/"+filename
        print("\n*********\n\nwritePlanToFile() file: "+str(filepath))
        with open(filepath, "w") as planFile:
            planFile.write("# file: "+filename+" "+timestamp+"\n")
            for key in self.stats.keys():
                if key == "horizonGp":
                    planFile.write("\n")
                planFile.write("\n"+key+": "+str(self.stats[key]))
            planFile.write("\n\nSuccess node: "+str(successNode))
            # planFile.write("\n\ns"+str(satId)+" nodes ("+str(len(satPlan))+"):\n")

            if self.horizonId > 1:
                if not self.isGapPlan:
                    priorTick = self.priorHorizonFinalTick
                    priorAngle = self.priorHorizonFinalAngle
            priorTick = self.horizonStart
            priorAngle = -1
            totalExtraTime = 0
            maxExtraTime  = 0
            maxExtraTimeTick = 0
            gapCount = 0
            satObservedGp = []
            satNodeCount = 0
            lastNode = None
            priorEnergy = None
            minEnergyStep = None
            lastVar = None
            for planStep in self.successNode.plan:
                if planStep:
                    var = planStep[1]
                    lastVar = var
                    varSatId = var.satId
                    if varSatId != satId:
                        continue
                    satNodeCount += 1
                    print("step "+str(planStep[0])+": "+str(planStep[1]))
                    tick = var.tick
                    assignment = var.assignment
                    cmd = assignment[0]
                    gpList = assignment[1]
                    satObservedGp.extend(gpList)
                    choiceInfo = dshieldUtil.parseChoice(cmd)
                    payload = choiceInfo['payload']
                    angle = choiceInfo['pointingOption']
                    payload2 = choiceInfo['payload2'] if 'payload2' in choiceInfo else None

                    # angle2 = choiceInfo['pointingOption2'] if 'pointingOption2' in choiceInfo else None
                    gapStart = priorTick + 1 # gap starts after observation
                    gapEnd = tick -1  # gap ends 1 tick before tick
                    gapDur = gapEnd - gapStart + 1
                    priorTick = tick
                    slewDur = 0
                    if priorAngle > 0:
                        slewDur, slewEnergy = self.getSlewTimeAndEnergy(priorAngle, angle)
                    startAngle = priorAngle
                    endAngle = angle
                    priorAngle = angle
                    if gapDur > 0:
                        gapCount += 1
                        gapMsg = "\n["+str(gapStart) + "-"+str(gapEnd)+"] gap ("+str(gapDur)+")"
                        if slewDur > 0:
                            slewEnd = gapEnd-1 # leave space for observation start after slewing
                            slewStart = slewEnd - slewDur + 1
                            gapMsg += ", slew: ["+str(slewStart)+"-"+str(slewEnd)+"] ("+str(slewDur)+")"
                        extraTime = gapDur - slewDur
                        if extraTime > maxExtraTime:
                            maxExtraTime = extraTime
                            maxExtraTimeTick = gapStart
                        totalExtraTime += extraTime
                        if slewDur > 0:
                            gapMsg += ", extra time: "+str(extraTime)
                        if gapDur > 1:
                            # insert observation finish active tokens at start of gap if necessary
                            if instrumentStates["L"]:
                                priorCmdFinishToken = instrumentStates["L"] + ".E"
                                instrumentStates["L"] = None
                                cmdFinishMsg = str(gapStart)+": "+priorCmdFinishToken+"\n"
                                planFile.write(cmdFinishMsg)
                                self.updateSensorState(satId, "L", gapStart, priorCmdFinishToken)

                            if instrumentStates["P"]:
                                priorCmdFinishToken = instrumentStates["P"] + ".E"
                                instrumentStates["P"] = None
                                cmdFinishMsg = str(gapStart)+":           "+priorCmdFinishToken+"\n"
                                planFile.write(cmdFinishMsg)
                                self.updateSensorState(satId, "P", gapStart, priorCmdFinishToken)

                        planFile.write(gapMsg+"\n\n")
                        gapEnd = gapStart + gapDur - 1
                        gapDict = {"gapStart": gapStart, "gapEnd": gapEnd, "gapDur": gapDur, "slewDur": slewDur, "startAngle": startAngle, "endAngle": endAngle, "satId": satId}
                        self.gaps[gapStart] = gapDict
                        self.satGaps[satId][gapStart] = gapDict
                        totalSlewTicks += slewDur
                        totalIdleTicks += extraTime

                    # payload1 command changed without gap
                    if instrumentStates[payload] != cmd:
                        # insert start token for new command
                        instrumentStates[payload] = cmd
                        cmdStartTick = tick-1
                        cmdStartMsg = str(cmdStartTick)+": "
                        newCmdStartToken = cmd + ".S"
                        if payload.startswith("P"):
                            cmdStartMsg += "          "
                        cmdStartMsg += newCmdStartToken+"\n"
                        planFile.write(cmdStartMsg)
                        self.updateSensorState(satId, payload, cmdStartTick, newCmdStartToken)

                        # insert stop tokens for prior cmd
                        if payload == "L" and instrumentStates["P"]:
                            priorCmdFinishToken = instrumentStates["P"] + ".E"
                            instrumentStates["P"] = None
                            cmdFinishMsg = str(tick)+":           "+priorCmdFinishToken+"\n"
                            planFile.write(cmdFinishMsg)
                            self.updateSensorState(satId, "P", tick, priorCmdFinishToken)
                        if payload == "P" and instrumentStates["L"]:
                            priorCmdFinishToken = instrumentStates["L"] + ".E"
                            instrumentStates["L"] = None
                            cmdFinishMsg = str(tick)+": "+priorCmdFinishToken+"\n"
                            planFile.write(cmdFinishMsg)
                            self.updateSensorState(satId, "L", tick, priorCmdFinishToken)

                    # payload2 command changed without gap
                    if payload2 and instrumentStates[payload2] != cmd:
                        # insert start token for new cmd
                        instrumentStates[payload2] = cmd
                        cmdStartTick = tick-1
                        cmdStartMsg = str(cmdStartTick)+": "
                        newCmdStartToken = cmd + ".S"
                        if payload.startswith("P"):
                            cmdStartMsg += "          "
                        cmdStartMsg += newCmdStartToken+"\n"
                        planFile.write(cmdStartMsg)
                        self.updateSensorState(satId, payload2, cmdStartTick, newCmdStartToken)

                        if payload2 == "L" and instrumentStates["P"]:
                            priorCmdFinishToken = instrumentStates["P"] + ".E"
                            instrumentStates["P"] = None
                            cmdFinishMsg = str(tick)+":           "+priorCmdFinishToken+"\n"
                            planFile.write(cmdFinishMsg)
                            self.updateSensorState(satId, "P", tick, priorCmdFinishToken)

                        if payload2 == "P" and instrumentStates["L"]:
                            priorCmdFinishToken = instrumentStates["L"] + ".E"
                            instrumentStates["L"] = None
                            cmdFinishMsg = str(tick)+": "+priorCmdFinishToken+"\n"
                            planFile.write(cmdFinishMsg)
                            self.updateSensorState(satId, "L", tick, priorCmdFinishToken)

                    msg = str(tick) + ": "
                    if cmd.startswith("P"):
                        msg += "          "
                    msg += cmd+"     "
                    if cmd.startswith("L"):
                        msg += "          "
                    msg += str(gpList)
                    self.updateSensorState(satId, cmd[0], tick, cmd + " "+str(gpList))
                    totalObsTicks += 3
                    # energy = self.getNodeEnergy(node.id, satPlan)
                    # assert energy, "writePlanToFile() ERROR! no energy for node: "+str(node)
                    # if energy:
                    #     if not minEnergyStep or energy < minEnergyStep['energy']:
                    #         planStep = self.getPlanStep(node.id, satPlan)
                    #         minEnergyStep = planStep
                    #     msg += ", energy "+str(round(energy, 3))
                    #     if priorEnergy:
                    #         if energy > priorEnergy:
                    #             msg += " [+]"
                    #         elif energy < priorEnergy:
                    #             msg += " [-]"
                    #         elif energy == self.energyMax:
                    #             msg += " [*]"
                    #     priorEnergy = energy
                    #     if energy < self.energyMin:
                    #         msg += " LOW!"
                    if self.isSatInEclipse(satId, tick):
                        msg += " eclipse"
                    msg += "\n"
                    planFile.write(msg)
            satObservedGpCount = len(satObservedGp)
            satUniqueObservedGpCount = len(list(set(satObservedGp)))
            satGpPerImage = round(satObservedGpCount/float(satNodeCount),3)
            satSummaryInfo = {'planSize': satNodeCount, 'observedGP': satObservedGpCount, 'satGpPerImage': satGpPerImage, 'unique Observed GP': satUniqueObservedGpCount, 'obsTicks': totalObsTicks, 'slewTicks': totalSlewTicks, 'idleTicks': totalIdleTicks}
            self.solutionSummary[satId] = satSummaryInfo
            planFile.write("\n\n~~~~~~~~~~~\n\n##### Summary for sat "+satName+":\n")
            msg = satName+" Plan summary: "+str(satSummaryInfo)
            if self.isGapPlan:
                minDepthOfCharge = round(minEnergyStep['energy'] / self.energyMax, 3)
                msg += "\n Lowest energy plan step: "+str(minEnergyStep)+ ", Lowest charge depth: "+ str(minDepthOfCharge)
                self.lowestEnergy[satId] = minDepthOfCharge
            print(msg)
            planFile.write("\n# "+msg)

            # planFile.write("\n# " + satName + " final plan node: "+str(lastNode.id)+", var: "+str(lastNode.var)+", nodeCount: "+str(satNodeCount))
            cmd = lastVar.assignment[0]
            finalCommand = dshieldUtil.parseChoice(cmd)
            finalTick = lastVar.tick
            # finalState = lastNode.state
            finalAngle = finalCommand['pointingOption']
            extraTimePercentage = (totalExtraTime/self.horizonDur) * 100
            avgExtraTime = round(totalExtraTime/gapCount, 3)
            # finalStateMsg = "# "+satName + " final observation: "+str({'tick': finalTick, 'angle': finalAngle}) #, 'state': finalState})
            # planFile.write("\n"+finalStateMsg)
            # print(finalStateMsg)
            planFile.write("\n# " + satName + " gap count: "+str(gapCount)+", Total extra time: "+str(totalExtraTime)+", "+str(round(extraTimePercentage,3))+"%, avgExtraTime: "+str(avgExtraTime)+", maxExtraTime: "+str(maxExtraTime)+", maxExtraTimeTick: "+str(maxExtraTimeTick)+"\n")
            totalFinalObs = self.observedGPs
            totalFinalUniqueFinalObs = list(set(totalFinalObs))
            totalGpPerImage = round(len(totalFinalObs)/float(len(self.plan)),3)
            planFile.write("\n\n##### Full Summary for all sats:\n")
            planFile.write("\n# totals for all sats: plan length (image count): "+str(len(self.plan))+", observed GP count: " + str(len(totalFinalObs))+", "+str(totalGpPerImage)+" gp/image, unique GP: " + str(len(totalFinalUniqueFinalObs))+"\n")
            sortedObs = str(sorted(totalFinalObs))[1:-1]

            # NOTE: The final output must contain line starting with '# Observed GP' for reading prior observations
            planFile.write("\n\n# Observed GP count (all sats): "+str(len(totalFinalObs))+"\n")
            planFile.write(str(sortedObs))

    def updateSensorState(self, satId, sensor, tick, newState):
        # called by writePlanToFile()
        satSensorStates = self.sensorStates[satId]
        sensorStates = satSensorStates[sensor]
        if tick in sensorStates:
            priorState = sensorStates[tick]
            newStateSuffix = newState[-1]
            priorStateSuffix = priorState[-1]
            # if newStateSuffix != "S" and newStateSuffix != "E" and priorStateSuffix == "E" and tick > 1:
            #     if newState == sensorStates[tick-2]:
            #         print("writePlanToFile() extraneous end token at "+str(tick)+"! prior: "+priorState+", new: "+newState)

            if priorState[:-1] == newState[:-1] and priorStateSuffix == "E" and newStateSuffix == "S":
                print("writePlanToFile() removing double booked Start/Finish at "+str(tick)+"! prior: "+priorState+", new: "+newState)
                del sensorStates[tick]
            else:
                print("writePlanToFile() ERROR! "+sensor+" double booked at "+str(tick)+"! prior: "+priorState+", new: "+newState)
        else:
            sensorStates[tick] = newState

    def writePrettyPlanToFile(self, satId, timestamp):
        filename = self.getHorizonFilenamePrefix(satId, self.horizonId)+"."+timestamp+".prettyPlan.txt"
        csvFilename = self.getHorizonFilenamePrefix(satId, self.horizonId)+"."+timestamp+".prettyPlan.csv"
        filepath = self.dataPath + self.experimentRun+"/"+filename
        csvFilepath = self.dataPath + self.experimentRun+"/"+csvFilename
        print("\n*********\n\nwritePrettyPlanToFile() file: "+str(filepath))
        satSensorStates = self.sensorStates[satId]
        Lstates = satSensorStates["L"]
        Pstates = satSensorStates["P"]
        gaps = self.satGaps[satId]
        satHeader = self.satEventHeaders[satId]
        with open(filepath, "w") as planFile:
            with open(csvFilepath, "w") as csvPlanFile:

                planFile.write("# file: "+filename+" "+timestamp+"\n")
                csvPlanFile.write("# file: "+csvFilename+" "+timestamp+"\n")
                for headerRow in satHeader:
                    csvPlanFile.write(headerRow+"\n")
                csvPlanFile.write("TP,mode,L-band,P-band,L-band GPs,P-band GPs,reward\n")
                for tick in range(self.horizonStart, self.horizonEnd+1):
                    row = {"TP": tick, "mode": "obs", "Lband": "", "Pband": "", "LGP": "", "PGP": "", "reward": ""}
                    gpListL = None
                    gpListP = None
                    Lstate = Lstates[tick] if tick in Lstates else None
                    Pstate = Pstates[tick] if tick in Pstates else None
                    LstateMsg = ""
                    PstateMsg = ""
                    if Lstate or Pstate:
                        msg = str(tick).rjust(4)+":  "
                        if Lstate:
                            gpListIndex = Lstate.find("[")
                            if gpListIndex >= 0:
                                gpListL = Lstate[gpListIndex:]
                                Lstate = Lstate[:gpListIndex]
                            LstateMsg = Lstate
                            row["Lband"] = Lstate.strip()
                        msg += LstateMsg.ljust(10)

                        if Pstate:
                            gpListIndex = Pstate.find("[")
                            if gpListIndex >= 0:
                                gpListP = Pstate[gpListIndex:].strip()
                                Pstate = Pstate[:gpListIndex].strip()
                            PstateMsg = Pstate
                            row["Pband"] = Pstate.strip()
                        msg += PstateMsg.ljust(10)
                        gpListMsg = "   "
                        if gpListL and gpListP:
                            gpListMsg += "L:"+gpListL + ", P:"+gpListP
                            gpList = str(gpListL).replace(",", " ")
                            gpList = " ".join(gpList.split())  # remove any duplicate spaces
                            row["LGP"] = gpList.strip()
                            gpList = str(gpListP).replace(",", " ")
                            gpList = " ".join(gpList.split())  # remove any duplicate spaces
                            row["PGP"] = gpList.strip()
                        elif gpListL:
                            gpListMsg += gpListL
                            gpList = str(gpListL).replace(",", " ")
                            gpList = " ".join(gpList.split())  # remove any duplicate spaces
                            row["LGP"] = gpList.strip()
                        elif gpListP:
                            gpListMsg += gpListP
                            gpList = str(gpListP).replace(",", " ")
                            gpList = " ".join(gpList.split())  # remove any duplicate spaces
                            row["PGP"] = gpList.strip()
                        reward = self.getCmdRewardForResultRow(row)
                        if isinstance(reward, float):
                            reward = round(reward,5)
                            msg += str(reward).rjust(10)
                            row["reward"] = reward

                        msg += gpListMsg
                        planFile.write(msg+"\n")
                        self.prettyPrintPlanCsvRow(csvPlanFile, row)

                    gapInfo = gaps[tick] if tick in gaps else None
                    if gapInfo:
                        gapDur = gapInfo["gapDur"]
                        slewDur = gapInfo["slewDur"]
                        if (gapDur == 1 and slewDur == 0) and not Lstate and not Pstate:
                            # gap duration = 1 with no slew and no S or E states at this tick
                            gapMsg =  str(tick).rjust(4) + ": "
                            planFile.write(gapMsg+"\n")
                            # csvPlanFile.write(str(tick)+"\n")
                            row["TP"] = tick
                            self.prettyPrintPlanCsvRow(csvPlanFile, row)
                        elif gapInfo["startAngle"] != gapInfo["endAngle"]:
                                # if slewing occurs then slew can't start till gapStart + 1 and must end before gapEnd
                                slewStart = tick+1
                                slewEnd = slewStart + gapInfo["slewDur"]
                                gapEnd = gapInfo["gapEnd"]
                                if gapDur - slewDur < 2:
                                    errMsg = "ERROR! slew doesn't fit within gap! slewStart: "+str(slewStart)+"slewEnd: "+str(slewEnd)+", gap: "+ str(gapInfo)
                                    print("writePrettyPlanToFile() "+errMsg)
                                    csvPlanFile.write(errMsg)
                                for slewTick in range(slewStart, slewEnd):
                                    gapMsg =  str(slewTick).rjust(4) + ":     <<<<<<<<<  SLEW  >>>>>>>>"
                                    planFile.write(gapMsg+"\n")
                                    # csvPlanFile.write(str(slewTick)+",slew\n")
                                    row = {"TP": slewTick, "mode": "slew", "Lband": "", "Pband": "", "LGP": "", "PGP": "", "reward": ""}
                                    self.prettyPrintPlanCsvRow(csvPlanFile, row)
                                for idleTick in range(slewEnd, gapInfo["gapEnd"]):
                                    # assumes last tick in gap is a startToken for next obs
                                    idleMsg = str(idleTick).rjust(4) + ": ------- IDLE --------"
                                    planFile.write(idleMsg+"\n")
                                    # csvPlanFile.write(str(idleTick)+",idle\n")
                                    row = {"TP": idleTick, "mode": "idle", "Lband": "", "Pband": "", "LGP": "", "PGP": "", "reward": ""}
                                    self.prettyPrintPlanCsvRow(csvPlanFile, row)


    def prettyPrintPlanCsvRow(self, csvPlanFile, row):
        tick = row["TP"]
        tick = tick - self.horizonStart
        if tick == 0:
            tick = "0"
        row["TP"] = tick
        for key in list(row.keys()):
            term = row[key]
            if term:
                if key == "reward":
                    # TODO: Why is self.rowRewards much higher than objective?
                    # analyzeResults() total err reduction (objective score): 177.51585
                    # rowRewards: 373.72532
                    self.rowRewards += term
                if isinstance(term, float) or isinstance(term, int):
                    term = str(term)
                else:
                    term = term.strip()
            else:
                term = ""
            csvPlanFile.write(term)
            if key != "reward":
                csvPlanFile.write(",")
        csvPlanFile.write("\n")

    def getCmdRewardForResultRow(self, row):
        Lstate = None
        Pstate = None
        Lband = row["Lband"]
        Pband = row["Pband"]
        tick = row["TP"]
        gpListL = self.convertGpListFromString(row["LGP"])
        gpListP = self.convertGpListFromString(row["PGP"])
        if Lband and not Lband.endswith(".S") and not Lband.endswith(".E"):
            Lstate = Lband
        if Pband and not Pband.endswith(".S") and not Pband.endswith(".E"):
            Pstate = Pband
        cmd = None
        gpList = []
        if Lstate and Pstate:
            cmd = Lstate +"."+Pstate
            gpList = list(set(gpListL+gpListP))
        elif Lstate:
            cmd = Lstate
            gpList = gpListL
        elif Pstate:
            cmd = Pstate
            gpList = gpListP
        if cmd or gpList:
            if not cmd:
                print("getCmdRewardForResultRow() ERROR! GP list without cmd! row; "+str(row))
            reward = self.getCmdReward(tick, cmd, gpList, self.valSelectorHeuristic)
            return reward
        else:
            return None

    def convertGpListFromString(self, listString):
        result =[]
        if listString:
            gpList = listString[1:-1].split(" ")
            for term in gpList:
                result.append(int(term))
        return result


    # def getCombinedSatPlan(self, satId):
    #     # caller: writePlanToFile
    #     #computes energy for each step
    #     satPlan = []
    #     priorStep = None
    #     for node in self.plan:
    #         if node.var:
    #             if node.var.satId == satId:
    #                 nodeId = node.id
    #                 tick = node.var.tick
    #                 cmd = node.var.assignment[0]
    #                 planStep = {'node': nodeId, 'tick': tick, 'cmd': cmd}
    #                 if not priorStep:
    #                    planStep.update({'energy': self.initialEnergy})
    #                 else:
    #                     self.updateEnergyForPlanStep(satId, planStep, priorStep)
    #                 satPlan.append(planStep)
    #                 priorStep = planStep
    #     return satPlan

    def parseGpDict(self, dict):
        gp = GP(dict["gp"], None, None, "0", "B1")  # no lat, lon
        gp.type = int(dict["type"])
        gp.biomeId = dict["biomeId"]
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

    def writePlanCommandsFile(self, plan):
        self.errorTable = dshieldUtil.readMeasurementErrorTables()
        allCommands = []
        for node in plan:
            var = node.var
            if var: # skip root node
                satId = var.satId
                start = var.tick
                end   = start + 2
                cmd = var.assignment[0]
                gpList = var.assignment[1]
                parsedChoice = dshieldUtil.parseChoice(cmd)
                payload = parsedChoice["payload"]
                pointingOption = parsedChoice["pointingOption"]
                errorCode = parsedChoice["errorCode"]
                errorTableRow = (errorCode, 0) if payload == "L" else (0, errorCode)
                # TODO: handle biome-dependent errors:
                error = dshieldUtil.getError(errorTableRow, self.errorTable[7])
                normalizedScore = dshieldUtil.getNormalizedScore(error)
                instrumentId = 1 if payload == "L" else 2
                cmdDict = {"@type": "TakeImage", "satelliteId": satId, "timeIndexStart": start, "timeIndexEnd": end}
                cmdDict["instrumentId"] = instrumentId
                cmdDict["satelliteOrientation"] = "0,0,0"  # TODO: plug in pointingOptionToAngle for middle column
                cmdDict["instrumentOrientation"] = "0,0,0"
                cmdDict["observationValue"] = normalizedScore
                positions = []
                positionCoordinates = []
                for gpi in gpList:
                    gp = self.getGP(gpi)
                    lat = gp.lat
                    lon = gp.lon
                    coordinates = [lon, lat, 0]
                    positionCoordinates.append(coordinates)
                    positions.append(gpi)
                observedPositions = {"@type": "cartographicDegrees", "cartographicDegrees": positionCoordinates}
                observedGP = {"@type": "position", "position": positions}
                cmdDict["observedPosition"] = observedPositions
                cmdDict["position"] = observedGP
                allCommands.append(cmdDict)
        jsonCmds = json.dumps(allCommands)
        filename = "commands."+str(self.horizonId)+".json"
        filepath = self.dataPath + self.experimentRun+"/"+ filename
        f = open(filepath, "w")
        f.write(str(jsonCmds))
        f.close()
        print("wrote "+str(len(allCommands))+" commands to file: "+filepath)

    def readPriorObservations(self, satId, hId):
        fileprefix = self.getHorizonFilenamePrefix(satId, hId)
        filepath = self.dataPath +self.experimentRun+"/"
        filename = dshieldUtil.getPriorObservationFilename(filepath, fileprefix)
        filepath += filename
        horizonObsCount = 0
        if os.path.exists(filepath):
            print("readPriorObservations() reading prior observations from file: "+filename)
            with open(filepath, "r") as f:
                readingGP = False
                for line in f:
                    line = line.strip()
                    if line.startswith("# Final observation:"):
                        prefixSize = len("# Final observation:")
                        finalObs = line[prefixSize:].strip()
                        finalObs = ast.literal_eval(finalObs)
                        finalTick = finalObs['finalTick']
                        finalAngle = finalObs['finalAngle']
                        print("readPriorObservations() final tick: "+str(finalTick)+", finalAngle: "+str(finalAngle))
                        self.priorHorizonFinalTick = finalTick
                        self.priorHorizonFinalAngle = finalAngle

                    if line.startswith("# Observed GP"):
                        readingGP = True
                    elif not line.startswith("#"):
                        if readingGP:
                            terms = line.split(",")
                            count = len(terms)
                            print("parsed "+str(count) +" terms")
                            for term in terms:
                                self.priorGPs.append(int(term))
                                horizonObsCount += 1
            priorSet = set(self.priorGPs)
            print("read "+str(horizonObsCount) + " obs from horizon "+str(hId)+", "+str(len(self.priorGPs)) + " total prior observations, set: "+str(len(priorSet)))
        else:
            print("readPriorObservations() file not found: "+filepath)

    def readEclipseFiles(self):
        for satId in self.satList:
            self.readEclipseFileForSat(satId)

    def readEclipseFileForSat(self, satId):
        print("readEclipseFilesForSat() sat: "+str(satId)+", horizon: "+str(self.horizonId))
        if satId not in self.satEclipses:
            self.satEclipses[satId] = []
        path = self.dataPath+"preprocessing/sat"+str(satId)+"/"
        assert os.path.exists(path), "readEclipseFileForSat() ERROR! path not found: "+path
        eclipseFiles = [f for f in os.listdir(path) if "eclipse" in f]
        eclipseTimes = self.satEclipses[satId]
        for file in eclipseFiles:
            filepath = path + file
            print("reading eclipse  file: "+filepath)
            with open(filepath, "r") as f:
                for line in f:
                    line = line.strip()
                    if not line.startswith("start"):
                        if line.count(",") > 0:
                            terms = line.split(",")
                            start = self.horizonStart + int(terms[0])
                            end   = self.horizonStart + int(terms[1])
                            eclipse = {"start": start, "end": end}
                            eclipseTimes.append(eclipse)
        result = self.satEclipses[satId]
        print("Eclipses for s"+str(satId)+": "+str(result))

    def readFlatHorizonFile(self, satId):
        filename = self.getHorizonFilenamePrefix(satId, self.horizonId) + ".flat.txt"
        filepath = self.dataPath + "preprocessing/sat"+str(satId)+"/" + filename
        print("\nreadFlatHorizonFile() Reading horizon file: " + filepath)
        if not os.path.exists(filepath):
            print("\nreadFlatHorizonData() ERROR! File not found: " + filepath + "\n")
            return
        lastDict = {}
        dictLines = ""
        lineCount = 0
        horizonEvents = {}
        totalTpChoices = 0
        maxTpChoices = 0
        maxTpChoicesTp = None
        maxTpChoicesCombos = None
        self.satEventsHeaders = {}
        satHeader = []
        with open(filepath, "r") as f:
            for line in f:
                lineCount += 1
                line = line.strip()
                if 1 <= lineCount and lineCount <= 3:
                    satHeader.append(line)
                # print("line: "+str(line))
                if line and not line.startswith("#"):
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
                        if self.fixedPointingOption:
                            choices = self.filterAccessTimesForFixedPointingOption(choices)
                            if not choices:
                                print("removing filtered access time: "+str(d[tp]))
                                continue
                        # strip off deprecated scores (last item in choice list)
                        for cmd in choices:
                            gpList = choices[cmd]
                            lastGp = gpList[-1]
                            if isinstance(lastGp, float):
                                choices[cmd] = gpList[:-1]
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
                            # if self.includeChoiceErrors:
                            #     gpList = gpList[:-1]
                            for gpi in gpList:
                                # if not self.includeChoiceErrors or isinstance(gpi, int):
                                self.horizonGPs.add(gpi)
                                if gpi not in self.gpDict:
                                    print("readFlatHorizonFile() ERROR! unknown gpi: "+str(gpi))
                                gp = self.gpDict[gpi]
                                if not gp:
                                    print("readFlatHorizonFile() ERROR! gp not found for gpi: "+str(gpi))
                                    gp = GP(gpi, None, None, 0, None) # no lat, lon, type
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
        # print("\ns"+str(satId)+" maxChoicesTp: " + str(maxChoiceTp))
        # print("  choices ("+str(len(maxChoiceChoices))+")")
        # for choice in maxChoiceChoices:
        #     print("   "+str(choice))
        # print("\n  combos ("+str(len(maxChoiceCombos))+")")
        # for choice in maxChoiceCombos:
        #     print("   "+str(choice))

        self.satEventHeaders[satId] = satHeader
        print("satHeader: s"+str(satId)+": "+str(self.satEventHeaders[satId]))

        print("lastDict: " + str(lastDict))
        self.satEvents[satId] = horizonEvents
#========================
#     Utilities

    def getPointingAngleFromChoice(self, choice):
        # if isinstance(choice, int):
        #     return choice
        terms = str(choice).split(".")
        return int(terms[1])

    def updateSearchDepth(self, node):
        if node.parent:
            parentNode = self.planner.getNode(node.parent)
            parentDepth = parentNode.state["depth"] if "depth" in parentNode.state else 0
            node.state["depth"] = parentDepth + 1

    def getVarsForTime(self, node, t):
        #  multiple sats may have action at same time (though likely not same GP at same time)
        vars = []
        for var in node.unassignedVars:
            if var.tick == t:
                vars.append(var)
        return vars

    def removeEmptyVars(self, node):
        varsToRemove = []
        for v in node.unassignedVars:
            if not self.varHasChoices(v):
                varsToRemove.append(v)
        for v in varsToRemove:
            node.unassignedVars.remove(v)

    def varHasChoices(self, var):
        for choiceKey in var.choices:
            if var.choices[choiceKey]:
                return True
        return False

    def loadPreprocessingResults(self):
        self.readSlewTable()
        self.errorTable = dshieldUtil.readMeasurementErrorTables()
        self.initializeEvents()
        self.initialHorizonGpErrAvg = self.getInitialHorizonGpErrAvg()
        if self.useSortedGP:
            self.sortHorizonGps()

    def sortHorizonGps(self):
        # only called if self.useSortedGP = true (triage experiment cases)
        gpPairs = []
        for gpi in self.horizonGPs:
            modelErr = self.getGpModelErr(self.getGP(gpi), 0)
            gpPairs.append((gpi, modelErr))
        gpPairs.sort(key = lambda x: x[1], reverse = True)
        count = len(gpPairs)
        first = gpPairs[0]
        last = gpPairs[count-1]
        print("sortHorizonGps() original count: "+str(count)+", first: "+str(first)+", last: "+str(last))
        # cut the last half out
        maxCount = int(count * self.sortedGPpct)
        gpPairs = gpPairs[:maxCount]
        count = len(gpPairs)
        first = gpPairs[0]
        last = gpPairs[count-1]
        print("sortHorizonGps() final count: "+str(count)+", first: "+str(first)+", last: "+str(last))

        for gpPair in gpPairs:
            gpi = gpPair[0]
            err = gpPair[1]
            self.sortedHorizonGPs.append(gpi)
            self.sortedHorizonGPerr[gpi] = err
        # self.sortedHorizonGPs.sort()

    def getInitialHorizonGpErrAvg(self):
        totalErr = 0.0
        errCount = 0
        for gpi in self.horizonGPs:
            gp = self.getGP(gpi)
            modelErr = self.getGpModelErr(gp, self.horizonStart)
            totalErr += modelErr
            errCount += 1
        result = totalErr/errCount
        print("getInitialHorizonGpErrAvg() totalErr: "+str(round(totalErr, 5)) +", gpCount: "+str(errCount)+", avgErr: "+str(round(result, 5)))
        return result

    def getInitialObservedGpErrAvg(self):
        totalErr = 0
        for gpi in self.observedGPs:
            gp = self.getGP(gpi)
            totalErr += self.getGpModelErr(gp, self.horizonStart)
        gpCount = len(self.observedGPs)
        avg = dshieldUtil.roundIt(totalErr/gpCount)
        result = {"totalErr": dshieldUtil.roundIt(totalErr), "gpCount": gpCount, "avgErr": avg}
        return result

    def getFinalObservedGpErrAvg(self):
        totalErr = 0
        for gpi in self.observedGPs:
            gp = self.getGP(gpi)
            totalErr += gp.finalModelError
        gpCount = len(self.observedGPs)
        avg = dshieldUtil.roundIt(totalErr/gpCount)
        result = {"totalErr": dshieldUtil.roundIt(totalErr), "gpCount": gpCount, "avgErr": avg}
        return result

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


    def getGP(self, index):
        if index in self.gpDict:
            return self.gpDict[index]
        # else:
        #     print("getGP() ERROR! gp not found: " + str(index))

    def getHorizonGpFilenamePrefix(self, hId):
        hStart = ((hId - 1) * self.horizonDur) # #21600 #0 #21600
        hEnd = hStart + self.horizonDur - 1
        filename = str(hStart) + "-" + str(hEnd)
        return filename

    def getHorizonFilenamePrefix(self, satId, hId):
        hStart = ((hId - 1) * self.horizonDur) # #21600 #0 #21600
        hEnd = hStart + self.horizonDur - 1
        filename = "s" + str(satId) + "." + str(hStart) + "-" + str(hEnd)
        return filename

    def getEclipseFilenamePrefix(self):
        eclipseHour = (self.horizonId - 1) * 6
        filename = "eclipse_"+str(eclipseHour)+"hrs.csv"
        return filename

    def getObsCount(self, cmd):
        count = cmd.count(".")
        return count


if __name__ == '__main__':
    main()


