class Var:
    def __init__(self, name, choices, objective=0):
        self.name = name
        self.choices = choices
        self.assignment = None
        self.satId = None
        self.tick = None
        self.parseName()

    def parseName(self):
        satId, tick = self.name.split(".")
        self.satId = int(satId[1:])
        self.tick = int(tick)
        #self.name[2:]
        # name = self.name[2:] # broadcast
        # self.satId = int(name)
        # # TODO: remove domain-specific call to parseName
        # self.tick  = int(self.name[self.name.index(".") + 1:])

    def __str__(self):
        result = self.name
        if self.assignment:
            result += "="+str(self.assignment) #[0])
        else:
            result += ": "+str(list(self.choices.keys()))
            # result += ": "+str(self.choices)+"]"
        return result
