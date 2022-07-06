from node import Node
from var import Var
# from hamiltonVar import Var
import time
import gc

import copy

class Planit:
    def __init__(self):
        self.nodeBeamWidth = 1 # of nodes selected for expansion on each loop
        self.choiceBeamWidth = 1 #None (for a*/exhaustive) #1 # of choices selected on each node expansion (= # of child nodes created on each node expansion)
        self.nextNodeId = 0
        self.rootNode = None
        self.openNodes = list()
        self.allNodes = {}
        self.successNodes = list()
        # self.closedNodes = list()
        self.varSelector = None
        self.valSelector = None
        self.valSorter = None
        self.choicePropagator = None
        # self.gapEndPropagator = None
        # self.gapStartPropagator = None
        self.nodeSorter = None
        self.nodeScoringMethod = None
        self.stateUpdater = None
        self.successTest = None
        self.constraints = list()
        self.storeNodePlans    = True # default value
        # self.debug = True
        # self.vars = list()

    def solveIt(self):
        # isGcEnabled = gc.isenabled()
        # gcThresholds = gc.get_threshold()
        # gc.set_threshold(100)
        # gcThresholds = gc.get_threshold()
        nodeSorter = self.nodeSorterDefault
        if self.nodeSorter:
            nodeSorter = self.nodeSorter
        while self.openNodes and not self.successNodes:
            sortedNodes = nodeSorter()
            # print("sorted nodes ("+str(len(sortedNodes))+"), beamWidth: "+str(self.beamWidth))
            selectedNodes = sortedNodes[:self.nodeBeamWidth]
            for n in selectedNodes:
                # self.expandNodeBroadcast(n)
                self.expandNodeDshield(n)

        if self.successNodes:
            successNode = self.successNodes[0]
            print("\nsolveIt() Success! Solution node: "+str(successNode)+"\n")
            print("\nsolveIt() Success! Node Count: "+str(self.nextNodeId)+"\n")
            # print(self.printTree(self.rootNode, 0))
            # print(self.printTree(None, 0))
            return successNode
        else:
            print("\nsolveIt() INFEASIBLE!")

    def addVar(self, name, choices, objective=0):
        root = self.rootNode if self.rootNode else self.createNode()
        v = self.rootNode.addVar(name, choices, objective)
        # self.vars.append(v)

    def createNode(self):
        n = Node(self.getNextNodeId())
        if not self.rootNode:
            self.rootNode = n
        self.openNodes.append(n)
        self.allNodes[n.id] = n
        return n

    def getNode(self, nodeId):
        result = None
        if nodeId:
            result = self.allNodes[nodeId]
        return result

    def getParentNode(self, node):
        if node.parent:
            return self.getNode(node.parent)

    def copyNode(self, n):
        child = Node(self.getNextNodeId())
        child.unassignedVars = n.unassignedVars.copy()
        child.state = copy.deepcopy(n.state)

        # stateCopy = {}
        # for key in n.state.keys():
        #     stateCopy[key] = copy.deepcopy(n.state[key])
        # child.state = stateCopy #copy.deepcopy(n.state)
        # child.plan = n.plan.copy() # TODO: remove this if unused
        child.var = n.var
        self.openNodes.append(child)
        self.allNodes[child.id] = child
        return child

    def createChildNode(self, parent):
        child = self.copyNode(parent)
        child.parent = parent.id
        parent.children.append(child.id)
        return child

    def createMultipleRootNodes(self):
        # split root into 4 children each starting at least 5 seconds apart
        root = self.rootNode

        child1 = self.createChildNode(root)
        child2 = self.createChildNode(root)
        child3 = self.createChildNode(root)
        child4 = self.createChildNode(root)
        child5 = self.createChildNode(root)
        self.updateNodeStatus(root, "exhausted root choices")

        tick = child1.unassignedVars[0].tick
        tick = self.removeVarsEarlierThanTick(child2, tick + 5)
        tick = self.removeVarsEarlierThanTick(child3, tick + 5)
        tick = self.removeVarsEarlierThanTick(child4, tick + 5)
        tick = self.removeVarsEarlierThanTick(child5, tick + 5)

    def removeVarsEarlierThanTick(self, node, tick):
        filteredVars = []
        nextTick = None
        for var in node.unassignedVars:
            if var.tick > tick:
                filteredVars.append(var)
                if not nextTick:
                    nextTick = var.tick
        node.unassignedVars = filteredVars
        return nextTick

    def varSelectorDefault(self, node):
        if node.unassignedVars:
            return node.unassignedVars[0]
        else:
            print("varSelectorDefault() No unassigned vars!")

    def valSelectorDefault(self, node, var):
        choices = var.choices
        firstKey = list(choices.keys())[0]
        return (firstKey, choices[firstKey])

    def valSorterDefault(self, node, var):
        result = []
        choices = var.choices
        for choice in choices.keys():
            result.append((choice, choices[choice]))
        return result

    def nodeSorterDefault(self):
        searchStrategy = "dfs"
        if self.openNodes:
            nodeIndex = 0 # breadthFirst
            if searchStrategy == "dfs":
                nodeIndex = len(self.openNodes)-1
            return list(self.openNodes[nodeIndex])
        else:
            print("nodeSorterDefault() No open nodes!")

    def expandNodeDshield(self, parent): #, gap=None):
        if parent.unassignedVars:
            # print("\nexpandNode() " + str(parent)) #.id)+", var: "+str(n.var)) #+", gap: "+str(gap))
            # print(self.printTree(self.rootNode, 0))
            varSelector = self.varSelector if self.varSelector else self.varSelectorDefault
            valSorter   = self.valSorter   if self.valSorter else self.valSorterDefault
            selectedVar = varSelector(parent)
            if selectedVar:
                varIndex = parent.unassignedVars.index(selectedVar)
                # create a child for each choice
                parentChoices = selectedVar.choices
                childChoices = []
                negativeRewardChoices = [] # choices with negative rewards
                if len(parentChoices) > 0:
                    # dshield obs planner specific: TODO: move this out of planIt
                    choiceTuples = valSorter(parent, selectedVar) # returns sorted tuples of (cmd, gpList, reward) or (receiver1, receiver2) for broadcast
                    if not choiceTuples:
                        print("expandNode() rankedChoice pruned all choices! removing var from node: "+str(parent))
                        parent.unassignedVars.remove(selectedVar)
                        return
                    # dshield obs planner specific: TODO: move this out of planIt
                    for choiceTuple in choiceTuples:
                        choice = choiceTuple[0]
                        choiceReward = choiceTuple[2]
                        if choiceReward <= 0:
                            negativeRewardChoices.append(choiceTuple)
                    # remove choices with negative reward
                    # TODO: remove only those GP with negative reward. Same command may produce positive rewards for some GP and negative for others
                    if negativeRewardChoices:
                        print("expandNodeDshield() ERROR! negative reward choices: "+str(negativeRewardChoices))
                        y = 7 / 0
                        for negativeRewardChoice in negativeRewardChoices:
                            choiceTuples.remove(negativeRewardChoice)
                    if not choiceTuples:
                        if negativeRewardChoices:
                            print("expandNode() all choices were negative rewards! Pruned all choices! Removing var from node: "+str(parent))
                        else:
                            print("expandNode() valSorter pruned all choices! removing var from node: "+str(parent))
                        parent.unassignedVars.remove(selectedVar)
                        return
                    # trim choices to beamwidth
                    if self.choiceBeamWidth:
                        choiceTuples = choiceTuples[:self.choiceBeamWidth] #self.beamWidth]
                    else: # A* expands all children for var, never pick two vars for a single node
                        self.updateNodeStatus(parent, "exhausted")
                    for choiceTuple in choiceTuples:
                        choice = choiceTuple
                        if len(choiceTuple) > 2:
                            # dshield obs planner specific TODO: move this out of planIt
                            choice = choiceTuple[0]
                        childChoices.append(choice)
                        child = self.copyNode(parent)
                        child.depth = parent.depth + 1
                        child.parent = parent.id
                        child.plan = copy.copy(parent.plan)
                        # find matching var for var in copied child
                        for unassignedVar in child.unassignedVars:
                            if unassignedVar.name == selectedVar.name:
                                childVar = copy.copy(unassignedVar)
                                child.unassignedVars.remove(unassignedVar)
                                childVar.assignment = choiceTuple
                                child.var = childVar

                                # propagate choice
                                if self.choicePropagator:
                                    self.choicePropagator(child, childVar)
                                break
                            # else:
                            #     print("expandNode() ERROR! var not found: "+str(v))
                        if self.storeNodePlans: # True by default
                            child.plan.append((parent.id, childVar))
                        parent.children.append(child.id)
                        print("expandNode() parent: " + str(parent.id) +" -> child: " + str(child)+", choice: "+str(choice))
                        status, statusMsg = self.updateNodeState(child, selectedVar, choiceTuple)
                        if status:
                            if self.testConstraints(child):
                                self.updateNodeScore(child)
                                # TODO: move domain-specific logic out of planIt.
                                # A* (dynamic programming): Prune nodes if same dist but higher cost
                                # f = cost + dist
                                # cost = # of tp/obs assigned
                                # dist = # of GP (TP?) remaining

                                if self.successTest and self.successTest(child):
                                    print("expandNode() SUCCESS! Goal state achieved: " + str(child))
                                    self.updateNodeStatus(child, "success")
                        else:
                            print("pruning infeasible node: "+str(child))
                            self.updateNodeStatus(child, "failed", statusMsg)
                          #  return
                    # remove choice from parent's choice list (copy parent's var so change isn't inherited by children via pass by ref)
                    parentVarCopy = copy.deepcopy(selectedVar)
                    parentChoices = parentVarCopy.choices
                    parent.unassignedVars[varIndex] = parentVarCopy
                    for negativeRewardChoice in negativeRewardChoices:
                        parentChoices.pop(negativeRewardChoice[0])
                    for childChoice in childChoices:
                        # TODO: move domain-specific logic out of planIt.
                        parentChoices.pop(childChoice, None)  # removes choice from parent (n)

                    if not parentChoices:
                        # print("expandNode() exhausted all parent choices! removing var from node: "+str(parent))
                        parent.unassignedVars.remove(parentVarCopy)
                else:
                    # mark parent exhausted after spawning children
                    self.updateNodeStatus(parent, "exhausted")
            else:
                print("expandNode() ERROR no var for n " + str(parent))
        else:
            print("expandNode() Success!! No unassigned vars!")
            self.updateNodeStatus(parent, "success")

    def testConstraints(self, node):
        if self.constraints:
            for cons in self.constraints:
                if not cons(node): # call the constraint method with node as param
                    print("expandNode() failed Constraint for node: " + str(node))
                    explanation = cons.__name__
                    self.updateNodeStatus(node, "failed", explanation)
                    return False
        return True

    def updateNodeState(self, node, var, choice):
        if self.stateUpdater:
            return self.stateUpdater(node, var, choice)
        else:
            result = True
            statusMsg = None # errMsg for failure case
            return (result, statusMsg)

    def updateNodeScore(self, node):
        if self.nodeScoringMethod:
            self.nodeScoringMethod(node)

    def updateNodeStatus(self, n, status, statusMsg = ""):
        print("updateNodeStatus() n: "+str(n)+", status: "+str(status)+", msg: "+statusMsg)
        n.status = status
        if statusMsg:
            n.statusMsg = statusMsg
        if n in self.openNodes:
            self.openNodes.remove(n)
        if n.status == "success":
            self.successNodes.append(n)

    def isRootChild(self, node):
        return node.id == self.rootNode.id

    def addConstraint(self, constraint):
        self.constraints.append(constraint)

    def setInitialState(self, nodeState):
        root = self.rootNode if self.rootNode else self.createNode()
        root.state = nodeState

    def setStateUpdater(self, stateUpdater):
        self.stateUpdater = stateUpdater

    def setNodeScoringMethod(self, scoringMethod):
        self.nodeScoringMethod = scoringMethod

    def setNodeSorter(self, nodeSorter):
        self.nodeSorter = nodeSorter

    def setVarSelector(self, varSelector):
        self.varSelector = varSelector

    def setValSelector(self, valSelector):
        self.valSelector = valSelector

    def setValSorter(self, valSorter):
        self.valSorter = valSorter

    def setChoicePropagator(self, choicePropagator):
        self.choicePropagator = choicePropagator

    def setSuccessTest(self, successTest):
        self.successTest = successTest

    def getNextNodeId(self):
        self.nextNodeId += 1
        return self.nextNodeId


    def printTree(self, node, level):
        # RECURSIVE
        if node is None:
            # find root
            node = self.rootNode

        if node:
            msg = "\n"
            if level == 0:
                msg = "[root]\n"
                level += 1
            for i in range(level):
                msg += "  "
            msg += str(node)
            for childId in node.children:
                child = self.getNode(childId)
                # RECURSIVE !!!
                msg += self.printTree(child, level + 1)
            return msg
        else:
            print("printTree() *** ERROR *** root not found!")
