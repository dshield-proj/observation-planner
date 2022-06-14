from var import Var
# from hamiltonVar import Var

class Node:
    def __init__(self, nodeId):
        self.id = nodeId
        self.unassignedVars = list()
        # self.assignedVars = list()
        self.status = "open"
        self.statusMsg = None
        self.parent = None
        self.children = list()
        self.choice = None
        # self.score = None
        self.state = None
        self.plan = list()
        self.choiceReward = 0
        self.planReward = 0
        self.var = None
        self.depth = 0
        self.distance = 0
        # print("node() id: " + str(self.id))

    def addVar(self, name, choices, objective=0):
        v = Var(name, choices, objective)
        self.unassignedVars.append(v)
        return v

    def getAssignedVar(self, name):
        for v in self.assignedVars:
            if v.name == name:
                return v
        return None

    def collectReceivers(self, allSats):
        #returns list of pairs: (satId, receiver)
        result = list()
        for satId in allSats:
            satState = self.state[satId]
            receiver = satState["r"]
            if receiver:
                result.append((satId, receiver))
        return result

    def writeToDict(self):
        result = {"id": self.id}
        result["action"] = str(self.var)
        result.update(self.state)
        result["status"] = self.status
        if self.statusMsg:
            result["status"] += " "+self.statusMsg
        return result

    def __str__(self):
        msg = "["+str(self.id)+" "
        # vars = ""
        # kids = ""
        # if self.assignedVars:
        #     vars = str(self.assignedVars[-1])
        # for v in self.assignedVars:
        #     if vars:
        #         vars += ", "
        #     vars += str(v)
        # for v in self.unassignedVars:
        #     if vars:
        #         vars += ", "
        #     vars += str(v)
        # if self.var:
        msg += str(self.var)
        msg += ", plan score: " + str(round(self.planReward, 3))+", cost:" + str(self.depth)+", distance: "+str(self.distance)
        # msg += ", planReward: " + str(round(self.planReward, 3))+", choiceReward:" + str(round(self.choiceReward, 3))+", parent: "+str(self.parent)
        if self.state:
            depth = None
            gpCount = None
            energy = None
            if "depth" in self.state:
                depth = self.state["depth"]
                msg += ", imageCount: "+str(depth)
            if "observedGp" in self.state:
                gpCount = len(self.state["observedGp"])
                msg += ", gpCount: "+str(gpCount)
            if "energy" in self.state:
                energy = self.state["energy"]
                msg += ", energy: "+str(energy) #format(energy, '.3f')
            if depth and gpCount and energy:
                msg += ", gp/obs: " + format((gpCount/depth), '.3f')
                # msg += ", energy/gp: " + format((energy/gpCount), '.3f')
                # msg += ", energy/obs: " + format((energy/depth), '.3f')
        # if self.state:
        #     msg += ", state: "+str(self.state)
        # msg += ", vars: "+str(len(self.unassignedVars))
        # msg += ", unassignedVars: " + str(len(self.unassignedVars))
        # msg += ". assignedVars: " + str(len(self.assignedVars))

        # if self.parent:
        #     msg += ", parent: "+str(self.parent.id)
        # if self.children:
        #     msg += ", children: "
        #     for c in self.children:
        #         if kids:
        #             kids += ", "
        #         kids += str(c.id)
        # msg += kids
        # if self.plan:
        #     plan = ""
        #     for choice in self.plan:
        #         if plan:
        #             plan += ", "
        #         plan += str(choice)
        #     msg +=", plan: "+str(plan)
        # if self.score:
        #     msg += ", score: "+str(self.score)
        msg += ", "+self.status
        if self.statusMsg:
            msg += " "+self.statusMsg
        msg += "]"
        return msg
